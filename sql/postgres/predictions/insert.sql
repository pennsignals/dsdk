with i_predictions as (
    insert into predictions (
        run_id,
        subject_id,
        score
    )
    select
        %(run_id)s,
        %(subject_id)s,
        %(score)s
    returning *
), i_evidence as (
    insert into features (
        id,
        is_animal,
        is_vegetable,
        is_mineral,
        greenish
    )
    select
        id,
        %(is_animal)s,
        %(is_vegetable)s,
        %(is_mineral)s,
        %(greenish)s
    from
        i_predictions
    returning *
)
select
    id,
    run_id,
    subject_id,
    score
from
    i_predictions
