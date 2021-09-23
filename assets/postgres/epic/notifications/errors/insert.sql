insert into epic_notification_errors (
    prediction_id,
    name,
    description
)
select
    %(prediction_id)s,
    %(name)s,
    %(description)s
returning *
