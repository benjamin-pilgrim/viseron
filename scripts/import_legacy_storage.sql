-- Import the c73e8b2a9104 fork into a disposable copy of the 0d7a91c6e482 DB.
-- Run with psql -v ON_ERROR_STOP=1 -f scripts/import_legacy_storage.sql.
-- The database name must end in _merge_copy. Never run against the live DB.
-- legacy_source contains read-only tables from the frozen legacy DB, plus
-- media_audit(path text PRIMARY KEY, size bigint, hls_init_hash text).
-- Audit only regular files at the original container paths. For segments, the
-- hash must identify the matching init-<hash>.mp4 sidecar prepared for cutover.
-- Missing, wrong-sized or unaudited files remain unavailable. Recovery copies must first be restored
-- to their intended paths before they can be audited as available.
-- Files receive new IDs/keys, so legacy public file URLs change. Current file
-- URLs and legacy recording/detection IDs are preserved.
-- This imports metadata only: it neither moves media nor deletes source data.
BEGIN;
DO $$ BEGIN
 IF current_database() NOT LIKE '%\_merge\_copy' ESCAPE '\' THEN
  RAISE EXCEPTION 'Import requires a disposable database ending in _merge_copy';
 END IF;
 IF (SELECT version_num FROM public.alembic_version) IS DISTINCT FROM '0d7a91c6e482' THEN
  RAISE EXCEPTION 'Unsupported destination schema';
 END IF;
 IF EXISTS (SELECT FROM legacy_source.pending_segment_moves) THEN
  RAISE EXCEPTION 'Resolve legacy pending segment moves before importing';
 END IF;
 IF EXISTS (SELECT FROM legacy_source.recordings WHERE end_time IS NULL) THEN
  RAISE EXCEPTION 'Legacy recordings must be closed before importing';
 END IF;
END $$;
LOCK TABLE public.files, public.file_locations, public.file_key_aliases,
 public.recordings, public.motion, public.motion_contours, public.objects,
 public.post_processor_results, public.events IN EXCLUSIVE MODE;
CREATE SCHEMA IF NOT EXISTS legacy_import;
CREATE TABLE IF NOT EXISTS legacy_import.file_map (
 source_id integer PRIMARY KEY, target_id integer UNIQUE NOT NULL,
 source_file_key bigint NOT NULL, target_file_key bigint UNIQUE NOT NULL,
 source_path text NOT NULL, move_target jsonb
);
CREATE TABLE IF NOT EXISTS legacy_import.artifact_map (
 source_path text PRIMARY KEY, target_id integer NOT NULL
);
CREATE TABLE IF NOT EXISTS legacy_import.completed (
 singleton boolean PRIMARY KEY CHECK (singleton), source_counts jsonb NOT NULL
);
CREATE TEMP TABLE import_source_counts ON COMMIT DROP AS
 SELECT jsonb_object_agg(name, jsonb_build_array(n, max_id)) AS counts FROM (
 SELECT 'files' AS name, count(*) AS n, max(id) AS max_id FROM legacy_source.files
 UNION ALL SELECT 'recordings',count(*),max(id) FROM legacy_source.recordings
 UNION ALL SELECT 'motion',count(*),max(id) FROM legacy_source.motion
 UNION ALL SELECT 'motion_contours',count(*),max(id) FROM legacy_source.motion_contours
 UNION ALL SELECT 'objects',count(*),max(id) FROM legacy_source.objects
 UNION ALL SELECT 'post_processor_results',count(*),max(id) FROM legacy_source.post_processor_results
 UNION ALL SELECT 'events',count(*),max(id) FROM legacy_source.events
 ) s;
DO $$
DECLARE tab text; collision boolean; cols text; seq_name text; seq_value bigint;
BEGIN
 IF EXISTS (SELECT FROM legacy_import.completed) THEN
  IF (SELECT source_counts FROM legacy_import.completed) IS DISTINCT FROM
     (SELECT counts FROM import_source_counts) THEN
   RAISE EXCEPTION 'Legacy source differs from completed import';
  END IF;
  RETURN;
 END IF;
 FOREACH tab IN ARRAY ARRAY['recordings','motion','motion_contours','objects',
                            'post_processor_results','events'] LOOP
  EXECUTE format('SELECT EXISTS (SELECT FROM legacy_source.%I s JOIN public.%I t USING(id))',tab,tab) INTO collision;
  IF collision THEN RAISE EXCEPTION 'ID collision in %; no changes imported',tab; END IF;
 END LOOP;
 IF EXISTS (
  SELECT FROM legacy_source.files s JOIN public.files t
   ON t.camera_identifier=s.camera_identifier AND t.category=s.category
   AND t.subcategory=s.subcategory AND t.logical_path=CASE
    WHEN s.category='recorder' AND s.subcategory IN ('segments','event_clips')
     THEN substr(s.path,length(rtrim(s.tier_path,'/'))+2) ELSE s.filename END
 ) THEN RAISE EXCEPTION 'Overlapping logical files; resolve before importing'; END IF;

 INSERT INTO legacy_import.file_map
 SELECT s.id, (base.id + row_number() OVER (ORDER BY s.id))::integer,
  s.file_key, base.key + row_number() OVER (ORDER BY s.id), s.path, s.move_target
 FROM legacy_source.files s CROSS JOIN (
  SELECT greatest(coalesce((SELECT max(id) FROM public.files),0),
                  coalesce((SELECT max(id) FROM legacy_source.files),0)) AS id,
   greatest(coalesce((SELECT max(file_key) FROM public.files),0),
            coalesce((SELECT max(file_key) FROM public.file_key_aliases),0),
            coalesce((SELECT max(file_key) FROM legacy_source.files),0)) AS key
 ) base;
 RAISE NOTICE 'File identity map ready; importing file metadata';
 INSERT INTO public.files(id,file_key,tier_id,tier_path,camera_identifier,
  category,subcategory,path,directory,filename,size,duration,hls_init_hash,
  orig_ctime,created_at,updated_at)
 SELECT m.target_id,m.target_file_key,s.tier_id,s.tier_path,s.camera_identifier,
  s.category,s.subcategory,s.path,s.directory,s.filename,s.size,s.duration,
  a.hls_init_hash,s.orig_ctime,s.created_at,s.updated_at
 FROM legacy_source.files s JOIN legacy_import.file_map m ON m.source_id=s.id
 LEFT JOIN legacy_source.media_audit a ON a.path=s.path AND a.size=s.size;
 RAISE NOTICE 'Importing physical file locations';
 INSERT INTO public.file_locations(file_id,tier_id,tier_path,path,directory,
  filename,size,state,created_at,updated_at)
 SELECT m.target_id,s.tier_id,s.tier_path,s.path,s.directory,s.filename,s.size,
  CASE WHEN a.path IS NOT NULL AND
   (s.subcategory <> 'segments' OR a.hls_init_hash IS NOT NULL)
   THEN 'available' ELSE 'unavailable' END,s.created_at,s.updated_at
 FROM legacy_source.files s JOIN legacy_import.file_map m ON m.source_id=s.id
 LEFT JOIN legacy_source.media_audit a ON a.path=s.path AND a.size=s.size;

 -- Some legacy detection snapshots were never registered as Files.
 -- Index audited snapshots so current event APIs can return their file URLs.
 PERFORM setval(pg_get_serial_sequence('public.files','id'),greatest(
  coalesce((SELECT max(id) FROM public.files),0),
  (SELECT last_value FROM public.files_id_seq),1),true);
 PERFORM setval('public.files_file_key_seq',greatest(
  coalesce((SELECT max(file_key) FROM public.files),0),
  coalesce((SELECT max(file_key) FROM public.file_key_aliases),0),
  (SELECT last_value FROM public.files_file_key_seq),1),true);
 CREATE TEMP TABLE import_snapshots ON COMMIT DROP AS
 SELECT camera_identifier,snapshot_path,min(created_at) AS orig_ctime FROM (
  SELECT camera_identifier,snapshot_path,created_at FROM legacy_source.motion
  UNION ALL SELECT camera_identifier,snapshot_path,created_at FROM legacy_source.objects
  UNION ALL SELECT camera_identifier,snapshot_path,created_at FROM legacy_source.post_processor_results
 ) refs WHERE snapshot_path IS NOT NULL GROUP BY camera_identifier,snapshot_path;
 IF EXISTS (SELECT FROM import_snapshots WHERE snapshot_path NOT LIKE '/snapshots/%'
  OR snapshot_path LIKE '%/../%') THEN
  RAISE EXCEPTION 'Unsupported legacy snapshot path';
 END IF;
 INSERT INTO public.files(tier_id,tier_path,camera_identifier,category,subcategory,
  path,directory,filename,size,orig_ctime)
 SELECT 0,'/',r.camera_identifier,'snapshots',split_part(r.snapshot_path,'/',3),
  r.snapshot_path,regexp_replace(r.snapshot_path,'/[^/]+$',''),
  regexp_replace(r.snapshot_path,'^.*/',''),a.size,
  coalesce(r.orig_ctime,now() AT TIME ZONE 'utc')
 FROM import_snapshots r JOIN legacy_source.media_audit a ON a.path=r.snapshot_path
 WHERE NOT EXISTS (SELECT FROM public.files f WHERE f.path=r.snapshot_path);
 INSERT INTO legacy_import.artifact_map
 SELECT r.snapshot_path,f.id FROM import_snapshots r JOIN public.files f
  ON f.path=r.snapshot_path AND f.camera_identifier=r.camera_identifier
  AND f.category='snapshots';
 INSERT INTO public.file_locations(file_id,tier_id,tier_path,path,directory,
  filename,size,state,created_at,updated_at)
 SELECT f.id,f.tier_id,f.tier_path,f.path,f.directory,f.filename,f.size,
  'available',f.created_at,f.updated_at FROM public.files f
 JOIN legacy_import.artifact_map m ON m.target_id=f.id
 WHERE NOT EXISTS (SELECT FROM public.file_locations l WHERE l.file_id=f.id);

 RAISE NOTICE 'Importing recordings and detection history';
 -- Copy common columns explicitly, excluding the destination's new file links.
 FOREACH tab IN ARRAY ARRAY['motion','objects','post_processor_results',
                            'motion_contours','recordings','events'] LOOP
  SELECT string_agg(format('%I',c.column_name),',' ORDER BY c.ordinal_position)
   INTO cols FROM information_schema.columns c
   WHERE c.table_schema='public' AND c.table_name=tab
    AND c.column_name IN (SELECT column_name FROM information_schema.columns
                         WHERE table_schema='legacy_source' AND table_name=tab);
  -- FDW maps the legacy enum onto the destination's existing enum type.
  EXECUTE format('INSERT INTO public.%I (%s) SELECT %s FROM legacy_source.%I',tab,cols,cols,tab);
 END LOOP;
 RAISE NOTICE 'Linking recording artifacts';
 UPDATE public.recordings r SET thumbnail_file_id=f.id
 FROM public.files f JOIN legacy_import.file_map m ON m.target_id=f.id
 WHERE r.id IN (SELECT id FROM legacy_source.recordings)
  AND f.camera_identifier=r.camera_identifier AND f.subcategory='thumbnails'
  AND f.filename=regexp_replace(r.thumbnail_path,'^.*/','');
 UPDATE public.recordings r SET clip_file_id=f.id
 FROM public.files f JOIN legacy_import.file_map m ON m.target_id=f.id
 WHERE r.id IN (SELECT id FROM legacy_source.recordings) AND f.path=r.clip_path;
 FOREACH tab IN ARRAY ARRAY['motion','objects','post_processor_results'] LOOP
  EXECUTE format('UPDATE public.%I t SET snapshot_file_id=f.id FROM public.files f WHERE t.id IN (SELECT id FROM legacy_source.%I) AND f.path=t.snapshot_path AND (f.id IN (SELECT target_id FROM legacy_import.file_map) OR f.id IN (SELECT target_id FROM legacy_import.artifact_map))',tab,tab);
 END LOOP;
 -- Keep future inserts and public keys beyond both imported and current rows.
 FOREACH tab IN ARRAY ARRAY['files','file_locations','recordings','motion',
  'motion_contours','objects','post_processor_results','events'] LOOP
  seq_name := pg_get_serial_sequence('public.'||tab,'id');
  EXECUTE format('SELECT last_value FROM %s',seq_name) INTO seq_value;
  EXECUTE format('SELECT setval(%L,greatest(coalesce(max(id),0),%s,1),true) FROM public.%I',seq_name,seq_value,tab);
 END LOOP;
 PERFORM setval('public.files_file_key_seq',greatest(
  coalesce((SELECT max(file_key) FROM public.files),0),
  coalesce((SELECT max(file_key) FROM public.file_key_aliases),0),
  (SELECT last_value FROM public.files_file_key_seq),1),true);
 INSERT INTO legacy_import.completed SELECT true,counts FROM import_source_counts;
END $$;
SELECT count(*) AS imported_files,
 count(*) FILTER (WHERE l.state='unavailable') AS unavailable_files
 FROM legacy_import.file_map m JOIN public.file_locations l ON l.file_id=m.target_id;
COMMIT;
