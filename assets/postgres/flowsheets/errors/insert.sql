with args as (
    select
        cast(%(prediction_id)s as int) as prediction_id,
        cast(%(description)s as varchar) as description,
        cast(%(dry_run)s as int) as dry_run,
        cast(%(name)s as varchar(64)) as name,
        int8range(cast(%(profile_on)s as bigint), cast(%(profile_end)s as bigint)) as profile,
        cast(%(status_code)s as int) as status_code,
        cast(%(text)s as varchar) as text
)
insert into flowsheet_errors
    (prediction_id, description, name, profile, status_code, text)
select
    a.prediction_id,
    a.description,
    a.name,
    a.profile,
    a.status_code,
    a.text
from
    args as a
where
    a.dry_run = 0
returning *
