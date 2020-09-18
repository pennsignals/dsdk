set search_path = test,public;

create or replace function up()
returns void as $$
    -- dependency/alphabetic order
    create or replace function is_timezone(time_zone varchar)
    returns boolean as $function$
    declare valid timestamptz;
    begin
        valid := now() at time zone time_zone;
        return true;
    exception when invalid_parameter_value or others then
        return false;
    end;
    $function$ language plpgsql stable;
    -- timezone domain/column data type matches no-underscore convention here:
    create domain timezone as varchar
        check ( is_timezone(value) );
    create or replace function call_notify()
    returns trigger as $function$
    declare last_id text;
    begin
        select max(id)
        into last_id
        from inserted;
        perform pg_notify(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, last_id);
        return null;
    end;
    $function$ language plpgsql;
    create table if not exists models (
        id int primary key generated always as identity,
        version varchar not null,
        constraint model_version_must_be_unique
            unique (version)
    );
    create table if not exists microservices (
        id int primary key generated always as identity,
        version varchar not null,
        constraint microservice_version_must_be_unique
            unique (version)
    );
    -- `set timezone` for the session reinterprets all tztimestamp during select with the new time zone
    -- but the data stored in tztimestamp remains unambiguous
    create table if not exists runs (
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
            on update cascade,
        -- pick one of the following two constaints or the index
        constraint only_one_run_per_duration -- no overlaps or outstanding (crashed) runs
            exclude using gist (duration with &&),
        constraint only_one_run_per_duration_microservice_and_model -- simultaneous, blue-green deploys allowed
            exclude using gist (microservice_id with =, model_id with =, duration with &&)
    );
    create index if not exists runs_duration_index on runs using gist (duration);
    create table if not exists predictions (
        id int primary key generated always as identity,
        run_id int not null,
        subject_id int not null,
        score double precision not null,
        constraint predictions_require_a_run
            foreign key (run_id) references runs (id)
            on delete cascade
            on update cascade,
        constraint only_one_prediction_per_subject_id_and_run
            unique (run_id, subject_id),
        -- pick one of the following two constaints
        constraint prediction_score_must_be_a_normal
            check (0.0 <= score and score <= 1.0),
        constraint prediction_score_must_be_a_valence
            check (-1.0 <= score and score <= 1.0)
    );
    drop trigger if exists predictions_inserted on predictions;
    create trigger predictions_inserted after insert on predictions
        referencing new table as inserted
        for each statement
        execute procedure call_notify();
$$ language sql;

create or replace function down()
returns void as $$
    drop table if exists models cascade;
    drop table if exists microservices cascade;
    drop function if exists call_notify;
    drop domain if exists timezone;
    drop function if exists is_timezone;
$$ language sql;

select up();
