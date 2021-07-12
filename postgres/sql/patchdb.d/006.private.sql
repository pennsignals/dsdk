set search_path = example;


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
$$
    language plpgsql
    set search_path = example;


create or replace function down_private()
returns void as $$
begin
    if not uninstall('private'::varchar) then
        return;
    end if;

    drop table features;
end;
$$
    language plpgsql
    set search_path = example;


select up_private();
