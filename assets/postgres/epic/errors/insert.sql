insert into epic_errors (
    prediction_id,
    error_name,
    error_description
)
select
    %(prediction_id)s,
    %(error_name)s,
    %(error_description)s
returning *
