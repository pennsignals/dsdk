with c as (
    select cast(%s as int) as id
)
update runs
set
    duration = tstzrange(lower(runs.duration), current_timestamp, '[)')
from
    c
where
    runs.id = c.id
