insert into epic_notifications (
    prediction_id
)
select
    %s
returning *
