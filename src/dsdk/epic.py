# -*- coding: utf-8 -*-
"""Epic."""

from base64 import b64encode
from contextlib import contextmanager
from datetime import datetime
from logging import getLogger
from select import select
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

from cfgenvy import yaml_type

from .postgres import Persistor as Postgres

try:
    from requests import Session
except ImportError:
    Session = None

logger = getLogger(__name__)


class Epic:  # pylint: disable=too-many-instance-attributes
    """Epic."""

    YAML = "!epic"

    @classmethod
    def as_yaml_type(cls, tag: Optional[str] = None):
        """As yaml type."""
        yaml_type(
            cls,
            tag or cls.YAML,
            init=cls._yaml_init,
            repr=cls._yaml_repr,
        )

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
        client_id: str,  # 00000000-0000-0000-0000-000000000000
        cookie: str,
        password: str,
        url: str,
        flowsheet_id: str,
        comment: str = "Not for clinical use.",
        contact_id_type: str = "CSN",
        flowsheet_id_type: str = "external",
        flowsheet_template_id: str = "3040005300",
        flowsheet_template_id_type: str = "external",
        lookback_hours: int = 72,
        patient_id_type: str = "UID",
        poll_timeout: int = 300,
        username: str = "Pennsignals",
        user_id_type: str = "external",
        user_id: str = "PENNSIGNALS",
    ):
        """__init__."""
        self.authorization = b"Basic " + b64encode(
            f"EMP${username}:{password}".encode("utf-8")
        )
        self.postgres = None
        self.client_id = client_id
        self.comment = comment
        self.contact_id_type = contact_id_type
        self.cookie = cookie
        self.flowsheet_id = flowsheet_id
        self.flowsheet_id_type = flowsheet_id_type
        self.flowsheet_template_id = flowsheet_template_id
        self.flowsheet_template_id_type = flowsheet_template_id_type
        self.lookback_hours = lookback_hours
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
                for each in listener:
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
            "patient_id_type": self.patient_id_type,
            "poll_timeout": self.poll_timeout,
            "url": self.url,
            "user_id": self.user_id,
            "user_id_type": self.user_id_type,
        }

    @contextmanager
    def listener(self):
        """Listener."""
        raise NotImplementedError()

    def listen(self, listen, cur, session):
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

    def on_notify(  # pylint: disable: unused-argument,no-self-use
        self,
        event,
        cur,
        session,
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

    def on_success(self, entity, cur, response):
        """On success."""
        raise NotImplementedError()

    def on_error(self, entity, cur, response):
        """On error."""
        raise NotImplementedError()

    def recover(self, cur, session):
        """Recover."""
        sql = self.postgres.sql
        cur.execute(sql.epic.prediction.recover)
        for each in cur.fetchall():
            ok, message = self.rest(each, session)
            if ok:
                self.on_success(each, cur, message)
            else:
                self.on_error(each, cur, message)

    def rest(self, entity, session):
        """Rest."""
        raise NotImplementedError()

    @contextmanager
    def session(self):
        """Session."""
        session = Session()
        session.verify = False
        session.headers.update(
            {
                "Authorization": self.authorization,
                "Content-Type": "application/json",
                "Cookie": self.cookie,
                "Epic-Client-ID": self.client_id,
                "Epic-User-ID": self.user_id,
                "Epic-User-IDType": self.user_id_type,
            }
        )
        yield session


class Notifier(Epic):
    """Notifier."""

    YAML = "!epicnotifier"

    @classmethod
    def as_yaml_type(cls, tag: Optional[str] = None):
        """As yaml type."""
        yaml_type(
            cls,
            tag or cls.YAML,
            init=cls._yaml_init,
            repr=cls._yaml_repr,
        )

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        return cls(**loader.construct_mapping(node, deep=True))

    @classmethod
    def _yaml_repr(cls, dumper, self, *, tag):
        """Yaml repr."""
        return dumper.represent_mapper(tag, self.as_yaml())

    @contextmanager
    def listener(self):
        """Listener."""
        postgres = self.postgres
        sql = postgres.sql
        with postgres.listen(sql.prediction.listen) as listener:
            yield listener

    def on_notify(self, event, cur, session):
        """On notify."""
        super.on_notify(event, cur, session)
        sql = self.postgres.sql
        cur.execute(sql.prediction.recent, event.id)
        for each in cur.fetchall():
            ok, message = self.rest(each, session)
            if ok:
                self.on_success(each, cur, message)
            else:
                self.on_error(each, cur, message)

    def on_success(self, notification, cur, response):
        """On success."""
        cur.execute(
            self.postgres.sql.epic.notification.insert,
            {"prediction_id": notification["id"]},
        )

    def on_error(self, notification, cur, response):
        """On error."""
        cur.execute(
            self.postgres.sql.epic.notification_error.insert,
            {
                "description": response.text,
                "name": response.reason,
                "prediction_id": notification["id"],
            },
        )

    def recover(self, cur, session):
        """Recover."""
        sql = self.postgres.sql
        cur.execute(sql.epic.notification.recover)
        for each in cur.fetchall():
            ok, message = self.rest(each, session)
            if ok:
                self.on_success(each, cur, message)
            else:
                self.on_error(each, cur, message)

    def rest(self, prediction, session) -> Tuple[bool, Any]:
        """Rest."""
        query = {
            "Comment": self.comment,
            "ContactID": prediction["csn"],
            "ContactIDType": self.contact_id_type,
            "FlowsheetID": self.flowsheet_id,
            "FlowsheetIDType": self.flowsheet_id_type,
            "FlowsheetTemplateID": self.flowsheet_template_id,
            "FlowsheetTemplateIDType": self.flowsheet_template_id_type,
            "InstantValueTaken": prediction["as_of"].strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "PatientID": prediction["empi"],
            "PatientIDType": self.patient_id_type,
            "UserID": self.user_id,
            "UserIDType": self.user_id_type,
            "Value": prediction["score"],
        }

        url = self.url.format(
            **{
                key: quote(value.encode("utf-8"))
                for key, value in query.items()
            }
        )

        print(url)

        response = session.post(
            url=url,
            data={},
        )

        return response.status_code == 200, response


class Verifier(Epic):
    """Verifier."""

    YAML = "!epicverifier"

    @contextmanager
    def listener(self):
        """Listener."""
        postgres = self.postgres
        sql = postgres.sql
        with postgres.listen(sql.notification.listen) as listener:
            yield listener

    def on_notify(self, event, cur, session):
        """On notify."""
        super.on_notify(event, cur, session)
        sql = self.postgres.sql
        cur.execute(sql.notification.recent, event.id)
        for each in cur.fetchall():
            ok, response = self.rest(each, session)
            if ok:
                self.on_success(each, session, response)
            else:
                self.on_error(each, session, response)

    def on_success(self, notification, cur, response):
        """On success."""
        cur.execute(
            self.postgres.sql.epic.verifications.insert,
            {"prediction_id": notification["id"]},
        )

    def on_error(self, notification, cur, response):
        """On error."""
        cur.execute(
            self.postgres.sql.epic.verifications.errors.insert,
            {
                "description": response.text,
                "name": response.reason,
                "notification_id": notification["id"],
            },
        )

    def rest(self, notification, session) -> Tuple[bool, Any]:
        """Rest."""
        json = {
            "ContactID": notification["csn"],
            "ContactIDType": self.contact_id_type,
            "FlowsheetRowIDs": [
                {
                    "ID": self.flowsheet_id,
                    "IDType": self.flowsheet_id_type,
                }
            ],
            "LookbackHours": self.lookback_hours,
            "PatientID": notification["empi"],
            "PatientIDType": self.patient_id_type,
            "UserID": self.user_id,
            "UserIDType": self.user_id_type,
        }
        response = session.post(
            url=self.url,
            json=json,
        )
        return response.status_code == 200, response


def test_notifier(csn, empi, score):
    """Rest."""
    from os import getcwd
    from os.path import join as pathjoin

    from cfgenvy import Parser

    from .asset import Asset

    Asset.as_yaml_type()
    Postgres.as_yaml_type()
    Notifier.as_yaml_type()
    parser = Parser()
    cwd = getcwd()

    notifier = parser.parse(
        argv=(
            "-c",
            pathjoin(cwd, "local", "test.notifier.yaml"),
            "-e",
            pathjoin(cwd, "secrets", "test.notifier.env"),
        )
    )

    prediction = {
        "as_of": datetime.utcnow(),
        "csn": csn,
        "empi": empi,
        "score": score,
    }

    with notifier.session() as session:
        ok, response = notifier.rest(prediction, session)
        print(ok)
        print(response.json())


def test_verifier(csn, empi, score):
    """Test verifier."""
    from os import getcwd
    from os.path import join as pathjoin

    from cfgenvy import Parser

    from .asset import Asset

    Asset.as_yaml_type()
    Postgres.as_yaml_type()
    Verifier.as_yaml_type()
    parser = Parser()
    cwd = getcwd()

    verifier = parser.parse(
        argv=(
            "-c",
            pathjoin(cwd, "local", "test.verifier.yaml"),
            "-e",
            pathjoin(cwd, "secrets", "test.verifier.env"),
        )
    )

    notification = {
        "as_of": datetime.utcnow(),
        "csn": csn,
        "empi": empi,
        "score": score,
    }

    with verifier.session() as session:
        ok, response = verifier.rest(notification, session)
        print(ok)
        print(response.json())


if __name__ == "__main__":
    test_notifier(
        csn="278820881",
        empi="8330651951",
        score="0.5",
    )
