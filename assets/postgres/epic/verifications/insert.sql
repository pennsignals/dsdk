insert into epic_verifications (
    notification_id
)
select
    %(notification_id)s
returning *
