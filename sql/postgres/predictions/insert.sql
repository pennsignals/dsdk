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
