with args as (
    select
        cast(null as int) as run_id,
        cast(null as int) as patient_id,
        cast(null as double precision) as score
    union all select
        %(run_id)s,
        %(patient_id)s,
        %(score)s
)
insert into predictions (
    run_id,
    patient_id,
    score
)
select
    run_id,
    patient_id,
    score
from
    args
where
    run_id is not null
returning *
