with c as (
    select
        cast(%s as semver) as microservice_version,
        cast(%s as semver) as model_version
), i_microservices as (
    insert into microservices (version)
    select microservice_version from c
    on conflict do nothing
    returning id, version
), i_models as (
    insert into models (version)
    select model_version from c
    on conflict do nothing
    returning id, version
), _microservices as (
    select cast('s' as char) as src, id, version
    from microservices as sm
    inner join c on c.microservice_version = sm.version
    union all
    select 'i', id, version
    from i_microservices
), _models as (
    select cast('s' as char) as src, id, version
    from models as sm
    inner join c on c.models_version = sm.version
    union all
    select 'i', id, version
    from i_models
)
insert into runs (microservice_id, model_id)
select
    _microservices.id,
    _models.id
from
    _microservices
    cross join _models
returning
    id, microservice_id, model_id, duration
