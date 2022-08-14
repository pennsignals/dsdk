"""Epic."""

from __future__ import annotations

from abc import ABC
from base64 import b64encode
from contextlib import contextmanager
from datetime import datetime
from json import JSONDecodeError, dumps
from time import sleep as default_sleep
from typing import TYPE_CHECKING, Any, Generator
from urllib.parse import urlencode

from cfgenvy import YamlMapping
from requests import Session
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout

from .interval import Interval, profile
from .persistor import Persistor
from .service import Service
from .utils import configure_logger, retry

logger = configure_logger(__name__)

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class SaveError(Exception):
    """Save error."""


class Result:  # pylint: disable=too-few-public-methods
    """Rest result."""

    def __init__(
        self,
        *,
        duration: Interval,
        status: bool,
        description: str | None = None,
        name: str | None = None,
        status_code: int | None = None,
        text: str | None = None,
    ):
        """__init__."""
        self.duration = duration
        self.status = status
        self.description = description
        self.name = name
        self.status_code = status_code
        self.text = text

    def __str__(self):
        """__str__."""
        return str(self.__dict__)


class Flowsheet(YamlMapping):  # pylint: disable=too-many-instance-attributes
    """Flowsheet."""

    TIMEOUT = dumps(
        {
            "key": "epic.rest.connection.or.timeout",
            "prediction": "%s",
        }
    )
    HTTP_ERROR = dumps(
        {
            "key": "epic.rest.http.error",
            "prediction": "%s",
        }
    )
    JSON_DECODE_ERROR = dumps(
        {"key": "epic.rest.json.decode.error", "prediction": "%s"}
    )
    SAVE_ERROR_MESSAGE = b"DATA_NOT_SAVED"
    SAVE_ERROR_BODY = b"There was an error filing data.  Data was not saved.."
    SUCCESS = dumps(
        {
            "key": "epic.rest",
            "prediction": "%s",
        }
    )

    YAML = "!flowsheets"

    def __init__(  # pylint: disable=too-many-locals
        self,
        *,
        client_id: str,
        cookie: str,
        flowsheet_id: str,
        flowsheet_template_id: str,
        password: str,
        url: str,
        username: str,
        contact_id_type: str = "CSN",
        flowsheet_id_type: str = "external",
        flowsheet_template_id_type: str = "external",
        operation_timeout: int = 5,
        patient_id_type: str = "UID",
        user_id: str = "PENNSIGNALS",
        user_id_type: str = "external",
    ):
        """__init__."""
        self.authorization = (
            b"Basic " + b64encode(f"EMP${username}:{password}".encode())
        ).decode("utf-8")
        self.client_id = client_id
        self.contact_id_type = contact_id_type
        self.cookie = cookie
        self.flowsheet_id = flowsheet_id
        self.flowsheet_id_type = flowsheet_id_type
        self.flowsheet_template_id = flowsheet_template_id
        self.flowsheet_template_id_type = flowsheet_template_id_type
        self.operation_timeout = operation_timeout
        self.password = password
        self.patient_id_type = patient_id_type
        self.url = url
        self.user_id = user_id
        self.user_id_type = user_id_type
        self.username = username

    def as_yaml(self) -> dict[str, Any]:
        """As yaml."""
        return {
            "client_id": self.client_id,
            "contact_id_type": self.contact_id_type,
            "cookie": self.cookie,
            "flowsheet_id": self.flowsheet_id,
            "flowsheet_id_type": self.flowsheet_id_type,
            "flowsheet_template_id": self.flowsheet_template_id,
            "flowsheet_template_id_type": self.flowsheet_template_id_type,
            "operation_timeout": self.operation_timeout,
            "password": self.password,
            "patient_id_type": self.patient_id_type,
            "url": self.url,
            "user_id": self.user_id,
            "user_id_type": self.user_id_type,
            "username": self.username,
        }

    def publish(self, persistor: Persistor):
        """Yield results from rest call for adding flowsheets."""
        sql = persistor.sql
        with persistor.rollback() as cur:
            missings = persistor.df_from_query(
                cur, sql.flowsheets.missing, parameters={"dry_run": 0}
            )
        with self.session() as session:
            for _, missing in missings.iterrows():
                result = self.rest(missing, session)
                with persistor.commit() as cur:
                    if result.status:
                        persistor.query(
                            cur,
                            sql.flowsheets.insert,
                            parameters={
                                "dry_run": 0,
                                "id": missing["id"],
                                "profile_end": result.duration.end,
                                "profile_on": result.duration.on,
                            },
                        )
                        continue
                    persistor.query(
                        cur,
                        sql.flowsheets.errors.insert,
                        parameters={
                            "description": result.description,
                            "dry_run": 0,
                            "name": result.name,
                            "prediction_id": missing["id"],
                            "profile_end": result.duration.end,
                            "profile_on": result.duration.on,
                            "status_code": result.status_code,
                            "text": result.text,
                        },
                    )
                yield result

    @contextmanager
    def session(self) -> Generator[Any, None, None]:
        """Session."""
        session = Session()
        session.verify = False
        session.headers.update(
            {
                "authorization": self.authorization,
                "content-type": "application/json",
                "cookie": self.cookie,
                "epic-client-id": self.client_id,
                "epic-user-id": self.user_id,
                "epic-user-idtype": self.user_id_type,
            }
        )
        yield session

    def rest(
        self,
        missing,
        session: Session,
    ) -> Result:
        """Rest."""
        query = {
            "Comment": missing["id"],
            "ContactID": missing["csn"],
            "ContactIDType": self.contact_id_type,
            "FlowsheetID": self.flowsheet_id,
            "FlowsheetIDType": self.flowsheet_id_type,
            "FlowsheetTemplateID": self.flowsheet_template_id,
            "FlowsheetTemplateIDType": self.flowsheet_template_id_type,
            "InstantValueTaken": missing["as_of"].strftime(DATETIME_FORMAT),
            "PatientID": missing["empi"],
            "PatientIDType": self.patient_id_type,
            "UserID": self.user_id,
            "UserIDType": self.user_id_type,
            "Value": missing["score"],
        }
        url = self.url + "?" + urlencode(query)
        try:
            with profile("dsdk.epic.rest") as interval:
                response = self.on_rest(
                    session, url, {}, self.operation_timeout
                )
            body = response.json()
            response.raise_for_status()
        except (RequestsConnectionError, Timeout) as e:
            logger.error(self.TIMEOUT, missing["id"])
            return Result(
                duration=interval,  # pylint: disable=used-before-assignment
                status=False,
                name=type(e).__name__,
                text=str(e),
            )
        except SaveError as e:
            logger.error(self.HTTP_ERROR, missing["id"])
            return Result(
                duration=interval,  # pylint: disable=used-before-assignment
                status=False,
                description="DATA_NOT_SAVED",
                name=type(e).__name__,
                status_code=400,
            )
        except HTTPError as e:
            logger.error(self.HTTP_ERROR, missing["id"])
            return Result(
                duration=interval,
                status=False,
                description=body[  # pylint: disable=used-before-assignment
                    "ExceptionMessage"
                ],
                name=type(e).__name__,
                status_code=response.status_code,
            )
        except JSONDecodeError as e:
            logger.error(self.JSON_DECODE_ERROR, missing["id"])
            return Result(
                duration=interval,
                status=False,
                name=type(e).__name__,
                status_code=response.status_code,
                text=response.text,  # this is also very verbose
            )
        logger.info(self.SUCCESS, missing["id"])
        return Result(
            duration=interval,
            status=True,
            description=body,
            status_code=response.status_code,
        )

    @retry((RequestsConnectionError, Timeout, SaveError))
    def on_rest(
        self,
        session: Session,
        url: str,
        json: dict[str, Any],
        timeout: int,
    ):
        """On post."""
        response = session.post(
            url=url,
            json=json,
            timeout=timeout,
        )
        if (
            (response.status_code != 400)
            or (response.content.count(self.SAVE_ERROR_MESSAGE) == 0)
            or (response.content.count(self.SAVE_ERROR_BODY) == 0)
        ):
            return response
        try:
            response.raise_for_status()
        except HTTPError as e:
            raise SaveError() from e
        raise RuntimeError(
            "Should not get here with raise_for_status and a status of 400."
        )

    def test(
        self,
        # csn=278820881,
        csn=218202909,  # inpatient admission date is 2019-02-06 at PAH
        # csn="BAD202909",
        empi="8330651951",
        # empi="BAD2345678",
        id=0,  # pylint: disable=redefined-builtin
        score="0.5",
    ):
        """Test epic API."""
        missing = {
            "as_of": datetime.utcnow(),
            "csn": csn,
            "empi": empi,
            "id": id,
            "score": score,
        }
        with self.session() as session:
            result = self.rest(missing, session)
        print(result)


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Mixin(BaseMixin):
    """Mixin."""

    @classmethod
    def yaml_types(cls) -> None:
        """Yaml types."""
        Flowsheet.as_yaml_type()
        super().yaml_types()

    @classmethod
    def publish_flowsheets(cls):
        """Publish flowsheets."""
        with cls.context("flowsheets.publish") as service:
            service.on_publish_flowsheets()

    @classmethod
    def publish_flowsheet(cls):
        """Flowsheets test."""
        with cls.context("flowsheets.publish") as service:
            service.on_publish_flowsheet()

    def __init__(
        self,
        *,
        poll_interval: int = 60,
        flowsheets: Flowsheet,
        **kwargs,
    ):
        """__init__."""
        self.poll_interval = poll_interval
        self.flowsheets = flowsheets
        super().__init__(**kwargs)

    def as_yaml(self) -> dict[str, Any]:
        """As yaml."""
        return {
            "flowsheets": self.flowsheets,
            "poll_interval": self.poll_interval,
            **super().as_yaml(),
        }

    def on_publish_flowsheet(self):
        """On publish flowsheet."""
        self.flowsheets.test()

    def on_publish_flowsheets(self, sleep=default_sleep):
        """On flowsheets."""
        while True:
            for _ in self.publish():
                pass
            sleep(self.poll_interval)

    def publish(self) -> Generator[Any, None, None]:
        """Publish."""
        raise NotImplementedError()
