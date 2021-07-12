select
    score
from
    predictions
where
    run_id = %(run_id)s
order by
    id desc;
