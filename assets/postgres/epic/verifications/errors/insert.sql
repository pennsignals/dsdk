insert into epic_verification_errors (
    notification_id,
    name,
    description,
)
select
    %(notification_id)s,
    %(name)s,
    %(description)s
returning *
