select
    p.id,
    p.run_id,
    p.csn,
    p.empi,
    p.score,
    r.as_of
from
    runs as r
    join predictions as p on
        p.run_id = r.id
        and upper(r.interval) != 'infinity'
    left join epic_notifications as n on
        n.prediction_id = p.prediction_id
    left join epic_notification_errors as e on
        e.prediction_id = p.prediction_id
        and e.acknowledged_on is null
where
    n.id is null
    and e.id is null
order by
    r.id, p.id
