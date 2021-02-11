set search_path = dsdk;


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


select up_epic();
