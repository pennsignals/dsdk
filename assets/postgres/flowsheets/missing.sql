with args as (
    select coalesce(cast(%(dry_run)s as int), 1) as dry_run
)
select
    m.*
from
    args as a
    cross join lateral missing_flowsheets(dry_run => a.dry_run) as m;
