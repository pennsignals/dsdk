set search_path = test,public;

create or replace function up()
returns void as $$
    -- dependency/alphabetic order
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
    create sequence if not exists models_sequence;
    create table if not exists models (
        id int default nextval('models_sequence') primary key,
        version semver not null,
        constraint model_version_must_be_unique
            unique (version)
    );
    create sequence if not exists microservices_sequence;
    create table if not exists microservices (
        id int default nextval('microservices_sequence') primary key,
        version semver not null,
        constraint microservice_version_must_be_unique
            unique (version)
    );
    create sequence if not exists runs_sequence;
    create table if not exists runs (
        id int default nextval('runs_sequence') primary key,
        microservice_id int not null,
        model_id int not null,
        -- see how tsrange interacts with drivers, python
        duration tstzrange not null default tstzrange(now(), 'infinity', '[)'),
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
    create sequence if not exists predictions_sequence;
    create table if not exists predictions (
        id int default nextval('predictions_sequence') primary key,
        run_id int not null,
        patient_id int not null,
        score double precision not null,
        constraint predictions_require_a_run
            foreign key (run_id) references runs (id)
            on delete cascade
            on update cascade,
        constraint only_one_prediction_per_patient_id_and_run
            unique (run_id, patent_id),
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
    drop sequence if exists predictions_sequence cascade;
    drop sequence if exists models_sequence cascade;
    drop sequence if exists microservices_sequence cascade;
    drop function if exists call_notify;
$$ language sql;

select up();
