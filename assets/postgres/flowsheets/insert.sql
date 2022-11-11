with args as (
    select
        cast(%(id)s as int) as id,
        cast(%(dry_run)s as int) as dry_run,
        int8range(cast(%(profile_on)s as bigint), cast(%(profile_end)s as bigint)) as profile
)
insert into flowsheets
    (id, profile)
select
    a.id,
    a.profile
from
    args as a
where
    a.dry_run = 0
returning *;
