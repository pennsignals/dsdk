with c as (
    select
        cast(%(run_id)s as int) as run_id,
        cast(%(patient_id)s as int) as patient_id,
        cast(%(score)s as double precision) as score,
), i as (
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
    c
returning id
