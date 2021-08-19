select
    n.id,
    p.id as prediction_id,
    p.csn,
    p.empi,
    p.score,
    r.as_of
from
    runs as r
    join predictions as p on
        p.run_id = r.id
        and upper(r.interval) != 'infinity'
    join epic_notifications as n on
        n.prediction_id = p.id
    left join epic_verifications as v on
        v.notification_id = n.id
    left join epic_verification_errors as e on
        e.notification_id = n.id
where
    v.id is null
    and e.id is null
order by
    p.id, n.id
