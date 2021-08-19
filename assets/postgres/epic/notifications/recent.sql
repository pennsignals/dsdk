with args as (
    select
        cast(%(notification_id)s as int) as notification_id
)
select
    n.id
    p.id as prediction_id,
    p.run_id,
    p.csn,
    p.empi,
    p.score,
    r.as_of
from
    args
    join runs as r
        upper(r.interval) != 'infinity'
    join predictions as p on
        p.run_id = r.id
    join epic_notifications as n on
        n.prediction_id = p.id
        and n.id <= args.notification_id
    left join epic_verifcations as v on
        v.notification_id = n.id
    left join epic_verification_errors as e on
        e.notification_id = n.id
        and e.acknowledged_on is null
where
    v.id is null
    and e.id is null
order by
    r.id, p.id
