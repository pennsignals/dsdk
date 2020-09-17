with args as (
    select
        false as extant,
        cast(null as varchar) as microservice_version,
        cast(null as varchar) as model_version,
        cast(null as timestamptz) as as_of,
        cast(null as float) as epoch_ms,
        cast(null as timezone) as time_zone
    union all select
        true,
        %(microservice_version)s,
        %(model_version)s,
        %(as_of)s,
        %(epoch_ms)s,
        %(time_zone)s
), i_microservices as (
    insert into microservices (
        version,
    )
    select
        microservice_version,
    from
        args
    where
        extant
    on conflict do nothing
    returning *
), i_models as (
    insert into models (
        version
    )
    select
        model_version
    from
        args
    where
        extant
    on conflict do nothing
    returning *
), si_microservices as (
    select
        cast('selected' as varchar) as src, id, version
    from
        microservices as sm
        inner join args
            on args.microservice_version = sm.version
    union all select
        'inserted', id, version
    from
        i_microservices
), si_models as (
    select
        cast('selected' as varchar) as src, id, version
    from
        models as sm
        inner join args
            on args.models_version = sm.version
    union all select
        'inserted', id, version
    from
        i_models
)
insert into runs (
    microservice_id,
    model_id,
    as_of,
    epoch_ms,
    time_zone
)
select
    si_microservices.id,
    si_models.id,
    args.as_of,
    args.epoch_ms,
    args.time_zone
from
    args
    cross join si_microservices
    cross join si_models
where
    args.extant
returning *
