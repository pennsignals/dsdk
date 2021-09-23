set search_path = example;


create or replace function up_epic()
returns void as $$
begin
    if not install('epic'::varchar, array['public']::varchar[]) then
        return;
    end if;

    create table epic_notifications (
        id int primary key generated always as identity,
        prediction_id int not null,
        assert_on timestamptz default statement_timestamp(),
        constraint only_one_epic_notification_per_prediction
            unique (prediction_id),
        constraint epic_notifications_require_a_prediction
            foreign key (prediction_id) references predictions (id)
            on delete cascade
            on update cascade
    );
    create trigger epic_notifications_inserted after insert on epic_notifications
        referencing new table as inserted
        for each statement
        execute procedure call_notify();

    create table epic_notification_errors (
        id int primary key generated always as identity,
        prediction_id int not null,
        assert_on timestamptz default statement_timestamp(),
        acknowledged_on timestamptz default null,
        name varchar,
        description varchar,
        constraint epic_notification_errors_require_a_prediction
            foreign key (prediction_id) references predictions (id)
            on delete cascade
            on update cascade
    );

    create table epic_verifications (
        id int primary key generated always as identity,
        notification_id int not null,
        assert_on timestamptz default statement_timestamp(),
        constraint only_one_epic_verification_per_notification
            unique (notification_id),
        constraint epic_verifications_require_a_notification
            foreign key (notification_id) references epic_notifications (id)
            on delete cascade
            on update cascade
    );
    create trigger epic_verifications_inserted after insert on epic_verifications
        referencing new table as inserted
        for each statement
        execute procedure call_notify();

    create table epic_verification_errors (
        id int primary key generated always as identity,
        notification_id int not null,
        assert_on timestamptz default statement_timestamp(),
        acknowledged_on timestamptz default null,
        name varchar,
        description varchar,
        constraint epic_verification_errors_require_a_notification
            foreign key (notification_id) references epic_notifications (id)
            on delete cascade
            on update cascade
    );

end;
$$
    language plpgsql
    set search_path = example;


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
$$
    language plpgsql
    set search_path = example;


select up_epic();
