update runs
set
    duration = tstzrange(lower(runs.duration), current_timestamp, '[)')
where
    runs.id = %(id)s
returning *
