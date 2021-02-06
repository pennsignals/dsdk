set search_path = dsdk;


create or replace function up_patch()
returns void as $$
begin
    create table if not exists patches (
        id varchar primary key,
        applied timestamptz not null default now(),
        applied_by varchar not null
    );
    create table if not exists patch_requires (
        patch_id varchar not null,
        require_id varchar not null,
        constraint patch_requires_have_natural_keys
            unique (patch_id, require_id),
        constraint patch_requires_belong_to_patches
            foreign key (patch_id)
            references patches (id)
            on update cascade
            on delete cascade,
        constraint patch_requires_have_many_patches
            foreign key (require_id)
            references patches(id)
    );

    begin
        create function install(in in_id varchar, in_requires varchar[])
        returns bool
        as $function$
        declare
            result int;
        begin
            lock table patches in exclusive mode;
            select 1 into result from patches where id = in_id;
            if found then
                return false;
            end if;
            raise notice 'Installing patch: %', in_id;
            insert into patches
                (id, applied_by)
            values
                (in_id, current_user);
            insert into patch_requires
                (patch_id, require_id)
            select
                in_id, unnest(in_requires);
            return true;
        end;
        $function$ language plpgsql;
    exception when duplicate_function then
    end;

    begin
        create function uninstall(in in_id varchar)
        returns bool
        as $function$
        declare
            result int;
        begin
            lock table patches in exclusive mode;
            select 1 into result from patches where id = in_id;
            if not found then
                return false;
            end if;
            raise notice 'Uninstalling patch: %', in_id;
            delete from patches where id = in_id;
            return true;
        end;
        $function$ language plpgsql;
    exception when duplicate_function then
    end;
    if not install('patch'::varchar, array[]::varchar[]) then
        return;
    end if;
end;
$$ language plpgsql;


create or replace function down_patch()
returns void as
$$
begin
    if not uninstall('patch'::varchar) then
        return;
    end if;
    drop table patch_requires;
    drop table patches;
end;
$$ language plpgsql;


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
    $function$ language plpgsql stable;

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
    $function$ language plpgsql;

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
$$ language plpgsql;


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
$$ language plpgsql;


create or replace function up_private()
returns void as $$
begin
    if not install('private'::varchar, array['public']::varchar[]) then
        return;
    end if;

    create table features (
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
    );
end;
$$ language plpgsql;


create or replace function down_private()
returns void as $$
begin
    if not uninstall('private'::varchar) then
        return;
    end if;

    drop table features;
end;
$$ language plpgsql;


create or replace function up_epic()
returns void as $$
begin
    if not install('epic'::varchar, array['public']::varchar[]) then
        return;
    end if;

    create table epic_notifications (
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

    create table epic_errors (
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
end;
$$ language plpgsql;

create or replace function down_epic()
returns void as
$$
begin
    if not uninstall('epic'::varchar) then
        return;
    end if;

    drop table epic_errors;
    drop table epic_notifications;
end;
$$ language plpgsql;



create or replace function up()
returns void as $$
    select up_patch();
    select up_public();
    select up_private();
    select up_epic();
$$ language sql;


create or replace function down()
returns void as $$
    select down_epic();
    select down_private();
    select down_public();
    select down_patch();
$$ language sql;


select up();
