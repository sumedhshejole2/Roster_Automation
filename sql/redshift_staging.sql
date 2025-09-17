CREATE TABLE public.roster_staging (
  providerid varchar(256),
  rosterdate timestamp,
  providername varchar(512),
  _source_file varchar(1024),
  _job_id varchar(128),
  _loaded_at timestamp default current_timestamp
);
