set search_path = example;

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
        $function$
            language plpgsql
            set search_path = example;
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
        $function$
            language plpgsql
            set search_path = example;
    exception when duplicate_function then
    end;

    if not install('patch'::varchar, array[]::varchar[]) then
        return;
    end if;
end;
$$
    language plpgsql
    set search_path = example;


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
$$
    language plpgsql
    set search_path = example;


select up_patch();
