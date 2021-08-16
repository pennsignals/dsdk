# -*- coding: utf-8 -*-
"""Epic."""

from contextlib import contextmanager
from logging import getLogger
from select import select
from typing import Any, Dict, Optional

from cfgenvy import yaml_type

from .postgres import Persistor as Postgres

try:
    from requests import Session
    from zeep import Client
    from zeep.transports import Transport
except ImportError:
    Client = None
    Session = None
    Transport = None

logger = getLogger(__name__)


class Interconnect:  # pylint: disable=too-many-instance-attributes
    """Interconnect."""

    YAML = "!interconnect"

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

    def __init__(
        self,
        *,
        authorization: str,
        cookie: str,
        postgres: Postgres,
        service_name: str,
        urn: str,
        wsdl: str,
        comment: str = "Not for clinical use.",
        connect_timeout: int = 5,
        contact_id_type: str = "csn",
        flowsheet_id: str = "3040015333",
        flowsheet_template_id: str = "3040005300",
        flowsheet_template_id_type: str = "internal",
        lookback_hours: int = 72,
        operation_timeout: int = 3,
        patient_id_type: str = "uid",
        user_id: str = "pennsignals",
        user_id_type: str = "external",
    ) -> None:
        """__init__."""
        self.authorization = authorization
        self.cookie = cookie
        self.postgres = postgres
        self.wsdl = wsdl
        self.comment = comment
        self.contact_id_type = contact_id_type
        self.connect_timeout = connect_timeout
        self.flowsheet_id = flowsheet_id
        self.flowsheet_template_id = flowsheet_template_id
        self.flowsheet_template_id_type = flowsheet_template_id_type
        self.lookback_hours = lookback_hours
        self.operation_timeout = operation_timeout
        self.patient_id_type = patient_id_type
        self.user_id = user_id
        self.user_id_type = user_id_type
        self.urn = urn

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {
            "authorization": self.authorization,
            "comment": self.comment,
            "contact_id_type": self.contact_id_type,
            "cookie": self.cookie,
            "flowsheet_id": self.flowsheet_id,
            "flowsheet_template_id": self.flowsheet_template_id,
            "flowsheet_template_id_type": self.flowsheet_template_id_type,
            "lookback_hours": self.lookback_hours,
            "patient_id_type": self.patient_id_type,
            "postgres": self.postgres,
            "urn": self.urn,
            "user_id": self.user_id,
            "user_id_type": self.user_id_type,
            "wsdl": self.wsdl,
        }

    @contextmanager
    def interconnect(self):
        """Interconnect soap client."""
        session = Session()
        session.headers.update(
            {
                "authorization": self.authorization,
                "cookie": self.cookie,
            }
        )
        session.verify = False
        transport = Transport(
            session=session,
            timeout=self.connect_timeout,
            opration_timeout=self.operation_timeout,
        )
        client = Client(
            wsdl=self.wsdl,
            service_name=self.service_name,
            transport=transport,
        )
        yield client

    def listen(self, listen, cur, interconnect):
        """Listen."""
        while True:
            readers, _, exceptions = select(
                [listen], [], [listen], self.epic.poll_timeout
            )
            if exceptions:
                break
            if not readers:
                continue
            if listen.poll():
                while listen.notifies:
                    yield listen.notified.pop()

    def on_notify(self, event, cur, intervconnect):
        """On postgres notify handler."""
        logger.debug(
            "NOTIFY: %(id)s.%(channel)s.%(payload)s",
            {
                "channel": event.channel,
                "id": event.id,
                "payload": event.payload,
            },
        )

    def soap(self, event, cur, interconnect):
        """Soap call."""
        raise NotImplementedError()


class Notifier(Interconnect):
    """Notifier."""

    YAML = "!interconnectnotifier"

    def __call__(self):
        """__call__."""
        postgres = self.epic.postgres
        sql = postgres.sql
        with self.interconnect() as interconnect:
            with postgres.listen(sql.prediction.listen) as listener:
                with postgres.commit() as cur:
                    self.recover(cur, interconnect)
                for each in listener:
                    self.on_notify(each, cur, interconnect)

    def on_notify(self, event, cur, interconnect):
        """On notify."""
        super.on_notify(event, cur, interconnect)
        sql = self.postgres.sql
        cur.execute(sql.prediction.recent, event.id)
        for each in cur.fetchall():
            self.soap(each, cur, interconnect)

    def recover(self, cur, interconnect):
        """Recover."""
        sql = self.epic.postgres.sql
        cur.execute(sql.epic.notification.recover)
        for each in cur.fetchall():
            self.soap(each, cur, interconnect)

    def soap(self, prediction, cur, interconnect):
        """Soap."""
        sql = self.postgres.sql
        print(dir(interconnect))
        response = interconnect.service.AddFlowsheetValue(
            Comment=self.comment,
            ContactID=prediction["csn"],
            ContactIDType=self.contact_id_type,
            FlowsheetID=self.flowsheet_id,
            FlowsheetTemplateID=self.flowsheet_template_id,
            FlowsheetTemplateTypeID=self.flowsheet_template_type_id,
            FlowsheetTypeID=self.flowsheet_type_id,
            InstantValueTaken=self.instant_value_taken,
            PatientID=prediction["empi"],
            PatientIDType=self.patient_id_type,
            UserID=self.user_id,
            UserIDType=self.user_id_type,
            Value=prediction["score"],
        )

        print(response)

        ok = response.Success
        if ok:
            cur.execute(
                sql.epic.notification.insert,
                {"prediction_id": prediction["id"]},
            )
        else:
            cur.execute(
                sql.epic.notification_error.insert,
                {
                    "description": response.text,
                    "name": response.reason,
                    "prediction_id": prediction["id"],
                },
            )


class Verifier(Interconnect):
    """Verifier."""

    YAML = "!interconnectverifier"

    def __call__(self):
        """__call__."""
        postgres = self.epic.postgres
        sql = postgres.sql
        with self.interconnect() as interconnect:
            with postgres.listen(sql.notification.listen) as listen:
                with postgres.commit() as cur:
                    self.recover(cur, interconnect)
                self.listen(listen, cur, interconnect)

    def on_notify(self, event, cur, interconnect):
        """On notify."""
        super.on_notify(event, cur, interconnect)
        sql = self.postgres.sql
        cur.execute(sql.notification.recent, event.id)
        for each in cur.fetchall():
            self.soap(each, cur, interconnect)

    def recover(self, cur, interconnect):
        """Recover."""
        sql = self.epic.postgres.sql
        cur.execute(sql.epic.verification.recover)
        for each in cur.fetchall():
            self.soap(each, cur, interconnect)

    def soap(self, notification, cur, interconnect):
        """Soap."""
        postgres = self.postgres
        sql = postgres.sql
        response = interconnect.GetFlowsheetRows(
            ContactID=notification["csn"],
            ContactTypeID=self.contact_type_id,
            FlowsheetRowIDs=[notification["flowsheet_row_id"]],
            LookBackHours=self.look_back_hours,
            PatientID=notification["empi"],
            PatienTypeID=self.patient_type_id,
            UserID=self.user_id,
            UserTypeID=self.user_type_id,
        )

        print(response)

        ok = response.Success

        if ok:
            cur.execute(
                sql.epic.verification.insert,
                {"notification_id": notification["id"]},
            )
        else:
            cur.execute(
                sql.epic.verification_error.insert,
                {
                    "description": response.text,
                    "name": response.reason,
                    "notification_id": notification["id"],
                },
            )


def main(empi, csn):
    """Main."""
    from os import getcwd
    from os.path import join as pathjoin

    from cfgenvy import Parser

    from .asset import Asset

    Asset.as_yaml_type()
    Notifier.as_yaml_type()
    Postgres.as_yaml_type()
    parser = Parser()
    cwd = getcwd()
    notifier = parser.parse(
        argv=(
            "-c",
            pathjoin(cwd, "local", "test.wsdl.yaml"),
            "-e",
            pathjoin(cwd, "secrets", "test.wsdl.env"),
        )
    )

    notification = {
        "csn": csn,
        "empi": empi,
        "score": 0,
    }

    with notifier.interconnect() as interconnect:
        with notifier.postgres.commit() as cur:
            notifier.soap(notification, cur, interconnect)


if __name__ == "__main__":
    main(
        csn="278820881",
        empi="833065951",
    )
