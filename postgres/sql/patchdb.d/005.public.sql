set search_path = example;


create or replace function up_public()
returns void as $$
begin
    if not install('public'::varchar, array['patch']::varchar[]) then
        return;
    end if;

    create or replace function is_timezone(time_zone varchar)
    returns boolean as $function$
    declare valid timestamptz;
    begin
        valid := now() at time zone time_zone;
        return true;
    exception when invalid_parameter_value or others then
        return false;
    end;
    $function$
        language plpgsql
        set search_path = example
        stable;

    create domain timezone as varchar
        check ( is_timezone(value) );

    create function call_notify()
    returns trigger as $function$
    declare last_id text;
    begin
        select max(id)
        into last_id
        from inserted;
        perform pg_notify(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, last_id);
        return null;
    end;
    $function$
        language plpgsql
        set search_path = example;

    create table models (
        id int primary key generated always as identity,
        version varchar not null,
        constraint model_version_must_be_unique
            unique (version)
    );

    create table microservices (
        id int primary key generated always as identity,
        version varchar not null,
        constraint microservice_version_must_be_unique
            unique (version)
    );

    -- `set timezone` for the session reinterprets all tztimestamp during select with the new time zone
    -- but the data stored in tztimestamp remains unambiguous
    create table runs (
        id int primary key generated always as identity,
        microservice_id int not null,
        model_id int not null,
        duration tstzrange not null default tstzrange((now() at time zone 'Etc/UTC'), 'infinity', '[)'),
        -- allow as-of to be in the past
        as_of timestamptz not null default (now() at time zone 'Etc/UTC'),
        -- allow run to use a non-utc timezone for selection criteria visit date/timestamp intervals
        -- time zone from the IANA (Olson) database
        -- time zone column name matches underscore convention here.
        time_zone timezone not null default 'America/New_York',
        constraint runs_require_a_microservice
            foreign key (microservice_id) references microservices (id)
            on delete cascade
            on update cascade,
        constraint runs_require_a_model
            foreign key (model_id) references models (id)
            on delete cascade
            on update cascade
        -- maybe pick one of the following two constaints on the index
        -- constraint only_one_run_per_duration -- no overlaps or outstanding (crashed) runs
        --    exclude using gist (duration with &&),
        -- constraint only_one_run_per_duration_microservice_and_model -- simultaneous, blue-green deploys allowed
        --    exclude using gist (microservice_id with =, model_id with =, duration with &&)
    );

    create index runs_duration_index on runs using gist (duration);
    create table predictions (
        id int primary key generated always as identity,
        run_id int not null,
        subject_id int not null,
        score double precision not null,
        constraint predictions_require_a_run
            foreign key (run_id) references runs (id)
            on delete cascade
            on update cascade,
        -- document your assumptions about how many predictions are made per subject
        -- per visit?
        -- per run?
        constraint only_one_prediction_per_subject_and_run
            unique (run_id, subject_id),
        -- pick one of the following two constaints
        constraint prediction_score_must_be_a_normal
            check (0.0 <= score and score <= 1.0),
        constraint prediction_score_must_be_a_valence
            check (-1.0 <= score and score <= 1.0)
    );

    create trigger predictions_inserted after insert on predictions
        referencing new table as inserted
        for each statement
        execute procedure call_notify();
end;
$$
    language plpgsql
    set search_path = example;


create or replace function down_public()
returns void as $$
begin
    if not uninstall('public'::varchar) then
        return;
    end if;

    drop trigger predictions_inserted on predictions;
    drop table predictions;
    drop table runs cascade;
    drop table microservices;
    drop table models cascade;
    drop function call_notify;
    drop domain timezone;
    drop function is_timezone;
end;
$$
    language plpgsql
    set search_path = example;


select up_public();
