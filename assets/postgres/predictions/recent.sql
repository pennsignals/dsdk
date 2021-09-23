with args as (
    select
        cast(%(prediction_id)s as int) as prediction_id
)
select
    p.id,
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
        p.id <= args.prediction_id
        and p.run_id = r.id
    left join epic_notifications as n on
        n.prediction_id = p.id
    left join epic_notification_errors as e on
        e.prediction_id = p.id
        and e.acknowledged_on is null
where
    n.id is null
    and e.id is null
order by
    r.id, p.id
