with c as (
    select
        cast(%s as int) as run_id,
        cast(%s as int) as csn,
        cast(%s as int) as pat_id,
        cast(%s as double precision) as score,
        cast(%s as double precision) as feature_1,
        cast(%s as double precision) as feature_2,
        cast(%s as double precision) as feature_n,
), i as (
    insert into predictions (
        run_id,
        csn,
        pat_id,
        score
    )
    select
        run_id,
        csn,
        pat_id,
        score
    from
        c
    returning id
)
insert into feature_vectors (id, feature_1, feature_2, feature_n)
select
    i.id,
    c.feature_1,
    c.feature_2,
    c.feature_n
from
    c
    cross join i
returning id
