insert into epic_notifications (
    prediction_id
)
select
    %(prediction_id)s
returning *
