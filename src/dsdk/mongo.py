from functools import partial
from functools import wraps

from pandas import DataFrame

from dsdk.utils import create_new_batch


# TODO: Make these wrappers classes to make them easier to customize?
def needs_batch_id(func):
    """Wrapper used to create a batch if it doesn't already exist."""

    def wrapper(self, *args, **kwargs):
        if not hasattr(self.batch, "batch_id"):
            self.batch.batch_id = create_new_batch(self.batch.mongo)
        return func(self, *args, **kwargs)

    return wrapper


# TODO: optional parameter to specify fields that aren't retained
def store_evidence(func=None, *, exclude_cols=None):
    if exclude_cols is None:
        exclude_cols = []
    exclude_cols = frozenset(exclude_cols)
    if func is None:
        return partial(store_evidence, exclude_cols=exclude_cols)

    @wraps(func)
    @needs_batch_id
    def wrapper(self, *args, **kwargs):
        evidence = func(self, *args, **kwargs)
        if isinstance(evidence, DataFrame):
            # TODO: We need to check column types and convert as needed
            evidence["batch_id"] = self.batch.batch_id
            evidence_keep = evidence[[c for c in evidence.columns if c not in exclude_cols]]
            res = self.batch.mongo[self.name].insert_many(evidence_keep.to_dict(orient="records"))
            assert evidence_keep.shape[0] == len(res.inserted_ids)  # TODO: Better exception
            evidence.drop(columns=["batch_id"], inplace=True)
        else:
            raise NotImplementedError(
                "Serialization is not implemented for type {}".format(type(evidence))
            )  # TODO: Is there a better way to handle this?
        return evidence

    return wrapper
