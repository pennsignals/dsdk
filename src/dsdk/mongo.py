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
def store_evidence(func):
    @needs_batch_id
    def wrapper(self, *args, **kwargs):
        evidence = func(self, *args, **kwargs)
        if isinstance(evidence, DataFrame):
            # TODO: We need to check column types and convert as needed
            evidence["batch_id"] = self.batch.batch_id
            res = self.batch.mongo[self.name].insert_many(evidence.to_dict(orient="records"))
            assert evidence.shape[0] == len(res.inserted_ids)
        else:
            raise NotImplementedError(
                "Serialization is not implemented for type {}".format(type(evidence))
            )  # TODO: Is there a better way to handle this?
        return evidence

    return wrapper
