# -*- coding: utf-8 -*-
"""Epic."""

from __future__ import annotations

from base64 import b64encode
from contextlib import contextmanager
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from json import dumps, loads
from logging import getLogger
from re import compile as re_compile
from select import select  # pylint: disable=no-name-in-module
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
    cast,
)
from urllib.parse import quote

from cfgenvy import Parser, yaml_type
from pkg_resources import DistributionNotFound, get_distribution
from requests import Session
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout

from .postgres import Persistor as Postgres
from .utils import configure_logger

try:
    __version__ = get_distribution("dsdk").version
except DistributionNotFound:
    # package is not installed
    pass


logger = getLogger(__name__)


DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Egress(Parser):  # pylint: disable=too-many-instance-attributes
    """Egress."""

    ON = dumps({"key": "%s.on"})
    END = dumps({"key": "%s.end"})
    YAML = "!egress"

    VERSION = __version__

    @classmethod
    def as_yaml_type(cls, tag: Optional[str] = None):
        """As yaml type."""
        Postgres.as_yaml_type()
        yaml_type(
            cls,
            tag or cls.YAML,
            init=cls._yaml_init,
            repr=cls._yaml_repr,
        )

    @classmethod
    @contextmanager
    def context(
        cls,
        key: str,
        argv: Optional[List[str]] = None,
        env: Optional[Mapping[str, str]] = None,
    ):
        """Context."""
        configure_logger("dsdk")
        logger.info(cls.ON, key)
        yield cls.parse(argv=argv, env=env)
        logger.info(cls.END, key)

    @classmethod
    def main(cls):
        """Main."""
        with cls.context("main") as service:
            service()

    @classmethod
    def yaml_types(cls):
        """Yaml types."""
        cls.as_yaml_type()

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        return cls(**loader.construct_mapping(node, deep=True))

    @classmethod
    def _yaml_repr(cls, dumper, self, *, tag):
        """Yaml repr."""
        return dumper.represent_mapper(tag, self.as_yaml())

    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        client_id: str,
        cookie: str,
        password: str,
        url: str,
        flowsheet_id: str,
        flowsheet_template_id: str,
        postgres: Postgres,
        user_id: str,
        username: str,
        comment: str = "Not for clinical use.",
        contact_id_type: str = "CSN",
        flowsheet_id_type: str = "external",
        flowsheet_template_id_type: str = "external",
        lookback_hours: int = 72,
        patient_id_type: str = "UID",
        poll_timeout: int = 60,
        operation_timeout: int = 5,
        user_id_type: str = "external",
    ):
        """__init__."""
        self.authorization = (
            b"Basic " + b64encode(f"EMP${username}:{password}".encode("utf-8"))
        ).decode("utf-8")
        self.postgres = postgres
        self.client_id = client_id
        self.comment = comment
        self.contact_id_type = contact_id_type
        self.cookie = cookie
        self.flowsheet_id = flowsheet_id
        self.flowsheet_id_type = flowsheet_id_type
        self.flowsheet_template_id = flowsheet_template_id
        self.flowsheet_template_id_type = flowsheet_template_id_type
        self.lookback_hours = lookback_hours
        self.operation_timeout = operation_timeout
        self.patient_id_type = patient_id_type
        self.poll_timeout = poll_timeout
        self.url = url
        self.user_id = user_id
        self.user_id_type = user_id_type

    def __call__(self):
        """__call__."""
        with self.session() as session:
            with self.listener() as listener:
                with self.postgres.commit() as cur:
                    self.recover(cur, session)
                for each in self.listen(listener):
                    self.on_notify(each, cur, session)

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {
            "authorization": self.authorization,
            "client_id": self.client_id,
            "comment": self.comment,
            "contact_id_type": self.contact_id_type,
            "cookie": self.cookie,
            "flowsheet_id": self.flowsheet_id,
            "flowsheet_id_type": self.flowsheet_id_type,
            "flowsheet_template_id": self.flowsheet_template_id,
            "flowsheet_template_id_type": self.flowsheet_template_id_type,
            "lookback_hours": self.lookback_hours,
            "operation_timeut": self.operation_timeout,
            "patient_id_type": self.patient_id_type,
            "poll_timeout": self.poll_timeout,
            "url": self.url,
            "user_id": self.user_id,
            "user_id_type": self.user_id_type,
        }

    @contextmanager
    def listener(self) -> Generator[Any, None, None]:
        """Listener."""
        raise NotImplementedError()

    def listen(self, listen) -> Generator[Any, None, None]:
        """Listen."""
        while True:
            readers, _, exceptions = select(
                [listen], [], [listen], self.poll_timeout
            )
            if exceptions:
                break
            if not readers:
                continue
            if listen.poll():
                while listen.notifies:
                    yield listen.notified.pop()

    def on_notify(  # pylint: disable=unused-argument,no-self-use
        self,
        event,
        cur,
        session: Session,
    ):
        """On postgres notify handler."""
        logger.debug(
            "NOTIFY: %(id)s.%(channel)s.%(payload)s",
            {
                "channel": event.channel,
                "id": event.id,
                "payload": event.payload,
            },
        )

    def on_success(self, entity, cur, content: Dict[str, Any]):
        """On success."""
        raise NotImplementedError()

    def on_error(self, entity, cur, content: Exception):
        """On error."""
        raise NotImplementedError()

    def recover(self, cur, session: Session):
        """Recover."""
        sql = self.postgres.sql
        cur.execute(sql.epic.prediction.recover)
        for each in cur.fetchall():
            ok, content = self.rest(each, session)
            if not ok:
                self.on_error(each, cur, cast(Exception, content))
                continue
            self.on_success(each, cur, cast(Dict[str, Any], content))

    def rest(
        self,
        entity,
        session: Session,
    ) -> Tuple[bool, Union[Exception, Dict[str, Any]]]:
        """Rest."""
        raise NotImplementedError()

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


class Notifier(Egress):
    """Notifier."""

    YAML = "!epicnotifier"

    @classmethod
    def test(
        cls,
        csn="278820881",
        empi="8330651951",
        id=0,  # pylint: disable=redefined-builtin
        score="0.5",
    ):
        """Test epic API."""
        prediction = {
            "as_of": datetime.utcnow(),
            "csn": csn,
            "empi": empi,
            "id": id,
            "score": score,
        }
        with cls.context("test.notifier.api") as notifier:
            with notifier.session() as session:
                ok, response = notifier.rest(prediction, session)
                print(ok)
                print(response)

    @contextmanager
    def listener(self) -> Generator[Any, None, None]:
        """Listener."""
        postgres = self.postgres
        sql = postgres.sql
        with postgres.listen(sql.predictions.listen) as listener:
            yield listener

    def on_notify(self, event, cur, session):
        """On notify."""
        super.on_notify(event, cur, session)
        sql = self.postgres.sql
        cur.execute(sql.prediction.recent, event.id)
        for each in cur.fetchall():
            ok, content = self.rest(each, session)
            if not ok:
                self.on_error(each, cur, cast(Exception, content))
                continue
            self.on_success(each, cur, cast(Dict[str, Any], content))

    def on_success(self, entity, cur, content: Dict[str, Any]):
        """On success."""
        cur.execute(
            self.postgres.sql.epic.notification.insert,
            {"prediction_id": entity["id"]},
        )

    def on_error(self, entity, cur, content: Exception):
        """On error."""
        cur.execute(
            self.postgres.sql.epic.notifications.errors.insert,
            {
                "description": str(content),
                "name": content.__class__.__name__,
                "prediction_id": entity["id"],
            },
        )

    def recover(self, cur, session: Session):
        """Recover."""
        sql = self.postgres.sql
        cur.execute(sql.epic.notification.recover)
        for each in cur.fetchall():
            ok, content = self.rest(each, session)
            if not ok:
                self.on_error(each, cur, cast(Exception, content))
                continue
            self.on_success(each, cur, cast(Dict[str, Any], content))

    def rest(
        self,
        entity,
        session: Session,
    ) -> Tuple[bool, Union[Exception, Dict[str, Any]]]:
        """Rest."""
        query = {
            "Comment": self.comment,
            "ContactID": entity["csn"],
            "ContactIDType": self.contact_id_type,
            "FlowsheetID": self.flowsheet_id,
            "FlowsheetIDType": self.flowsheet_id_type,
            "FlowsheetTemplateID": self.flowsheet_template_id,
            "FlowsheetTemplateIDType": self.flowsheet_template_id_type,
            "InstantValueTaken": entity["as_of"].strftime(DATETIME_FORMAT),
            "PatientID": entity["empi"],
            "PatientIDType": self.patient_id_type,
            "UserID": self.user_id,
            "UserIDType": self.user_id_type,
            "Value": entity["score"],
        }
        url = self.url.format(
            **{
                key: quote(value.encode("utf-8"))
                for key, value in query.items()
            }
        )
        try:
            response = session.post(
                url=url,
                json={},
                timeout=self.operation_timeout,
            )
            response.raise_for_status()
        except (RequestsConnectionError, HTTPError, Timeout) as e:
            return False, e
        return True, response.json()


class Verifier(Egress):
    """Verifier."""

    YAML = "!epicverifier"

    @classmethod
    def test(
        cls,
        csn="278820881",
        empi="8330651951",
        id=0,  # pylint: disable=redefined-builtin
        score="0.5",
    ):
        """Test verifier."""
        notification = {
            "as_of": datetime.utcnow(),
            "csn": csn,
            "empi": empi,
            "id": id,
            "score": score,
        }
        with cls.context("test.verifier.api") as verifier:
            with verifier.session() as session:
                ok, response = verifier.rest(notification, session)
                print(ok)
                print(response)

    @contextmanager
    def listener(self) -> Generator[Any, None, None]:
        """Listener."""
        postgres = self.postgres
        sql = postgres.sql
        with postgres.listen(sql.notification.listen) as listener:
            yield listener

    def on_notify(self, event, cur, session: Session):
        """On notify."""
        super().on_notify(event, cur, session)
        sql = self.postgres.sql
        cur.execute(sql.notification.recent, event.id)
        for each in cur.fetchall():
            ok, content = self.rest(each, session)
            if not ok:
                self.on_error(each, cur, cast(Exception, content))
                continue
            self.on_success(each, cur, cast(Dict[str, Any], content))

    def on_success(self, entity, cur, content: Dict[str, Any]):
        """On success."""
        cur.execute(
            self.postgres.sql.epic.verifications.insert,
            {"notification_id": entity["id"]},
        )

    def on_error(self, entity, cur, content: Exception):
        """On error."""
        cur.execute(
            self.postgres.sql.epic.verifications.errors.insert,
            {
                "description": str(content),
                "name": content.__class__.__name__,
                "notification_id": entity["id"],
            },
        )

    def rest(
        self,
        entity,
        session: Session,
    ) -> Tuple[bool, Union[Exception, Dict[str, Any]]]:
        """Rest."""
        json = {
            "ContactID": entity["csn"],
            "ContactIDType": self.contact_id_type,
            "FlowsheetRowIDs": [
                {
                    "ID": self.flowsheet_id,
                    "IDType": self.flowsheet_id_type,
                }
            ],
            "LookbackHours": self.lookback_hours,
            "PatientID": entity["empi"],
            "PatientIDType": self.patient_id_type,
            "UserID": self.user_id,
            "UserIDType": self.user_id_type,
        }
        try:
            response = session.post(
                url=self.url,
                json=json,
                timeout=self.operation_timeout,
            )
            response.raise_for_status()
        except (RequestsConnectionError, HTTPError, Timeout) as e:
            return False, e
        return True, response.json()


def parse_path(path: str) -> Tuple[str, Dict[str, str]]:
    """Parse path."""
    url, *query_list = path.split("?", 1)
    query = "".join(query_list)
    param_list = query.split("&")
    params: Dict[str, str] = {}
    for each in param_list:
        key, *values = each.split("=", 1)
        params[key] = "".join(values)
    return (url, params)


def equals(expected: Any) -> Callable[..., None]:
    """Equals."""

    def _check(key: str, value: Any):
        if value != expected:
            raise ValueError(f"{key} actual: {value}, expected: {expected}")

    return _check


def matches(expected: str) -> Callable[..., None]:
    """Matches."""
    pattern = re_compile(expected)

    def _check(key: str, value: str):
        if not pattern.match(value):
            raise ValueError(f"{key} actual: {value}, pattern: {expected}")

    return _check


def is_float(key: str, value: str):
    """Is float."""
    actual = str(float(value))
    if actual != value:
        raise ValueError(f"{key} actual: {value}, is not float")


def is_datetime(key: str, value: str):
    """Is datetime."""
    actual = datetime.strptime(value, DATETIME_FORMAT).strftime(
        DATETIME_FORMAT
    )
    if actual != value:
        raise ValueError(f"{key} actual: {value}, is not datetime")


def is_lookback(key: str, value: int):
    """Is lookback."""
    if not (0 <= value and value <= 72):
        raise ValueError(f"{key} actual {value}, is not (0 <= value <= 72")


class Server(HTTPServer):
    """Server.

    The errors returned do not match the API.
    """

    def __init__(
        self,
        server_address,
        handler_class,
        add_flowsheet_value_path: str,
        get_flowsheet_rows_path: str,
    ):
        """__init__."""
        super().__init__(server_address, handler_class)
        self.by_user: Dict[str, Any] = {}
        self.dispatch_url = {
            parse_path(path)[0].lower(): method
            for path, method in (
                (add_flowsheet_value_path, AddFlowsheetValue()),
                (get_flowsheet_rows_path, GetFlowsheetRows()),
            )
        }

    def __call__(self):
        """__call__."""
        self.serve_forever()


class AddFlowsheetValue:
    """AddFlowsheetValue."""

    def __call__(
        self, request, url, params, body,
    ):
        """__call__."""
        if self.is_invalid(request, url, params, body):
            return

        comment = params["Comment"]
        contact_id = params["ContectID"]
        contact_id_type = params["ContactIDType"].upper()
        flowsheet_id = params["FlowsheetID"]
        flowsheet_id_type = params["FlowsheetIDType"].upper()
        # flowsheet_template_id = params["FlowsheetTemplateID"]
        # flowsheet_template_id_type = params["FlowsheetTemplateIDType"]
        patient_id = params["PatientID"]
        patient_id_type = params["PatientIDType"].upper()
        user_id = params["UserID"]
        user_id_type = params["UserIDType"].upper()
        value = str(params["Value"])
        instant = params["InstantValueTaken"].isoformat() + 'Z'

        key = (flowsheet_id, flowsheet_id_type)
        by_patient = self.by_flowsheet.get(key)
        if by_patient is None:
            by_patient = self.by_flowsheet[key] = {}

        key = (patient_id, patient_id_type)
        by_contact = by_patient.get(key)
        if by_contact is None:
            by_contact = by_patient[key] = {}

        key = (contact_id, contact_id_type)
        rows = by_contact.get(key)
        if rows is None:
            rows = by_contact[key] = {
                "FlowsheetRows": [
                    {
                        "FlowsheetColumns": [],
                        "Name": "Test Message",
                        "Occurrence": 28,  # ???
                        "Unit": "",
                    },
                ],
            }

        flowsheet_columns = rows["FlowsheetRows"]["FlowsheetColumns"]

        entry = {
            "Comment": comment,
            "FlowsheetRowID": [
                {
                    "ID": flowsheet_id,
                    "Type": flowsheet_id_type,
                }
            ],
            "FormattedValue": value,
            "Instant": instant,
            "LinesDrainsAirwaysID": None,
            "OrderIDs": [],
            "RawValue": value,
            "UserEnteredBy": [
                {
                    "ID": user_id,
                    "Type": user_id_type,
                },
            ],
        }

        flowsheet_columns.append(entry)
        response = {
            "Errors": [],
            "Success": True,
        }
        return request.respond(200, response)

    def is_invalid(
        self, request, url, params, body,
    ) -> Optional[List[Exception]]:
        """Is invalid."""
        errors = request.check_headers()
        try:
            for key, validate in (
                ("Comment", None),
                ("ContactID", None),
                ("ContactIDType", None),
                ("FlowsheetID", None),
                ("FlowsheetIDType", None),
                ("FlowsheetTemplateID", None),
                ("FlowsheetTemplateIDType", None),
                ("PatientID", None),
                ("PatientIDType", equals("internal")),
                ("UserID", None),
                ("UserIDType", equals("internal")),
                ("Value", is_float),
                ("InstantValueTaken", is_datetime),
            ):
                value = params[key]
                if validate is not None:
                    validate(key, value)
        except (KeyError, ValueError) as e:
            errors.append(e)

        if not errors:
            return False

        # TODO:
        # - errors do not match the spec
        response = {
            "Errors": [
                f"{error.__class__.__name__}: {error}"
                for error in errors
            ],
            "Success": False,
        }
        request.respond(400, response)
        return True


class GetFlowsheetRows:
    """Get flowsheet rows."""

    def __call__(
        self, request, url, params, body,
    ):
        """__call__."""
        json = loads(body)
        if self.is_invalid(request, url, params, json):
            return

        contact_id = json["ContactID"]
        contact_id_type = json["ContactIDType"]
        patient_id = json["PatientID"]
        patient_id_type = json["PatientIDType"]
        # TODO: lookback_hours = json["LookbackHours"]
        # user_id = json["UserID"]
        # user_id_type = json["UserIDType"]
        flowsheet_row_ids = json["FlowsheetRowIDs"]

        # TODO FIXME

        default = {'FlowsheetRows': []}

        key = (flowsheet_id, flowsheet_id_type.lower())
        by_patient = self.by_flowsheet.get(key)
        if by_patient is None:
            return request.respond(request, 400, default)

        key = (patient_id, patient_id_type)
        by_contact = by_patient.get(key)
        if by_contact is None:
            return request.respond(request, 400, default)

        key = (contact_id, contact_id_type)
        body = by_contact.get(key)
        if body is None:
            return request.respond(request, 400, default)

        return request.respond(request, 200, body)

    def is_invalid(self, request, url, params, body):
        """Is invalid."""
        errors = request.check_headers()
        try:
            for key, validate in (
                ("ContactID", None),
                ("ContactIDType", None),
                ("FlowsheetID", None),
                ("FlowsheetIDType", None),
                ("LookbackHours", is_lookback),
                ("PatientID", None),
                ("PatientIDType", equals("internal")),
                ("UserID", None),
                ("UserIDType", equals("internal")),
            ):
                value = body[key]
                if validate is not None:
                    validate(key, value)
        except (KeyError, ValueError) as e:
            errors.append(e)

        try:
            key = "FlowsheetRowIDs"
            value = body[key]
            for each in value:
                _ = each["ID"]
                _ = each["IDType"]
        except (KeyError, ValueError) as e:
            errors.append(e)

        if not errors:
            return False

        response = {
            "Errors": [
                f"{error.__class__.__name__}: {error}"
                for error in errors
            ],
            "Success": False,
        }
        request.respond(400, response)
        return True


class RequestHandler(BaseHTTPRequestHandler):
    """Request Handler."""

    def do_POST(self):  # noqa: N802
        """Do post."""
        url, params = parse_path(self.path)
        url = url.lower()
        content_length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(content_length)
        self.server.dispatch[url](self, url, params, body)

    def check_headers(self) -> List[Exception]:
        """Check headers."""
        errors: List[Exception] = []
        for key, validate in (
            ("authorization", None),
            ("content-type", equals("application/json")),
            ("cookie", None),
            ("epic-client-id", matches(r"\d{8}-\d{4}-\d{4}-\d{4}-\d{12}")),
            ("epic-user-id", equals("PENNSIGNALS")),
            ("epic-user-idtype", equals("external")),
        ):
            try:
                value = self.headers.get(key, None)
                if validate:
                    validate(key, value)
            except (KeyError, ValueError) as e:
                errors.append(e)
        return errors

    def respond(self, code, response) -> None:
        """Respond."""
        self.send_response(code)
        self.send_headers("content-type", "application/json")
        self.end_headers()
        self.wfile.write(dumps(response).encode("utf-8"))
        self.wfile.close()
