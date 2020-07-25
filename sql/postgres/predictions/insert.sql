with c as (
    select
        cast(%s as int) as run_id,
        cast(%s as int) as patient_id,
        cast(%s as double precision) as score,
), i as (
    insert into predictions (
        run_id,
        patient_id,
        score
)
select
    run_id,
    csn,
    patient_id,
    score
from
    c
returning id
