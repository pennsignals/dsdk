set search_path = dsdk,public;


create or replace function up_public()
returns void as $$
begin
    -- towards idempotence, do not use "create or *replace*" inside.

    begin
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
    exception when duplicate_function then
        raise notice 'function "is_timezone" already exists, skipping';
    end;

    begin
        -- timezone domain/column data type matches no-underscore convention here:
        create domain timezone as varchar
            check ( is_timezone(value) );
    exception when duplicate_object then
        raise notice 'domain "timezone" already exists, skipping';
    end;

    begin
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
        $function$ language plpgsql;
    exception when duplicate_function then
        raise notice 'function "call_notify" already exists, skipping';
    end;

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
            on update cascade
        -- maybe pick one of the following two constaints on the index
        -- constraint only_one_run_per_duration -- no overlaps or outstanding (crashed) runs
        --    exclude using gist (duration with &&),
        -- constraint only_one_run_per_duration_microservice_and_model -- simultaneous, blue-green deploys allowed
        --    exclude using gist (microservice_id with =, model_id with =, duration with &&)
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

    begin
        create trigger predictions_inserted after insert on predictions
            referencing new table as inserted
            for each statement
            execute procedure call_notify();
    exception when duplicate_object then
        raise notice 'trigger "predictions_inserted" already exists, skipping';
    end;
end;
$$ language plpgsql;


create or replace function up_private()
returns void as $$
    create table if not exists features (
        id int primary key,
        greenish float not null,
        is_animal boolean not null,
        is_vegetable boolean not null,
        is_mineral boolean not null,
        constraint features_require_a_prediction
            foreign key (id) references predictions (id)
            on delete cascade
            on update cascade,
        constraint greenish_is_a_normal
            check ((0.0 <= greenish) and (greenish <= 1.0)),
        constraint kind_must_be_one_hot_encoded
            check (
                cast(is_animal as int)
                + cast(is_vegetable as int)
                + cast(is_mineral as int)
                = 1
            )
    )
$$ language sql;


create or replace function up_epic()
returns void as $$
    create table if not exists epic_notifications (
        id int primary key generated always as identity,
        prediction_id int not null,
        notified_on timestamptz default statement_timestamp(),
        constraint only_one_epic_notification_per_prediction
            unique (prediction_id),
        constraint prediction_epic_notifications_required_a_prediction
            foreign key (prediction_id) references predictions (id)
            on delete cascade
            on update cascade
    );

    create table if not exists epic_errors (
        id int primary key generated always as identity,
        prediction_id int not null,
        recorded_on timestamptz default statement_timestamp(),
        acknowledged_on timestamptz default statement_timestamp(),
        error_name varchar,
        error_description varchar,
        constraint prediction_epic_error_required_a_prediction
            foreign key (prediction_id) references predictions (id)
            on delete cascade
            on update cascade
    );
$$ language sql;


create or replace function up()
returns void as $$
    select up_public();
    select up_private();
    select up_epic();
$$ language sql;


create or replace function down()
returns void as $$
    drop table if exists epic_notifications;
    drop table if exists epic_errors;
    drop table if exists features;
    drop table if exists predictions;
    drop table if exists runs cascade;
    drop table if exists microservices;
    drop table if exists models cascade;
    drop function if exists call_notify;
    drop domain if exists timezone;
    drop function if exists is_timezone;
$$ language sql;


select up();
