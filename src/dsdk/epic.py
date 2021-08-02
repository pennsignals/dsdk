# -*- coding: utf-8 -*-
"""Epic."""

from logging import getLogger
from select import select
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen

from cfgenvy import yaml_type

from .postgres import Persistor as Postgres

logger = getLogger(__name__)


class Epic:
    """Epic."""

    YAML = "!epic"

    @classmethod
    def as_yaml_type(cls, *, tag: Optional[str] = None):
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
    def _yaml_repr(cls, dumper, self, *, tag: str):
        """Yaml repr."""
        return dumper.represent_mapping(tag, self.as_yaml())

    def __init__(
        self,
        *,
        authorization: str,
        cookie: str,
        postgres: Postgres,
        poll_timeout: int = 60,
        uri_timeout: int = 5,
        user_id: str = "pennsignals",
        user_id_type: str = "external",
    ) -> None:
        """__init__."""
        self.authorization = authorization
        self.cookie = cookie
        self.postgres = postgres
        self.poll_timeout = poll_timeout
        self.uri_timeout = uri_timeout
        self.user_id = user_id
        self.user_id_type = user_id_type

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {
            "authorization": self.authorization,
            "cookie": self.cookie,
            "poll_timeout": self.poll_timeout,
            "postgres": self.postgres,
            "uri_timeout": self.uri_timeout,
            "user_id": self.user_id,
            "user_id_type": self.user_id_type,
        }


class FlowsheetEgress:  # pylint: disable=too-many-instance-attributes
    """Flowsheet Egress."""

    YAML = "!flowsheetegress"

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
        epic: Epic,
        uri: str,
        flowsheet_id: str = "3040015333",
        flowsheet_template_id: str = "3040005300",
        flowsheet_template_id_type: str = "internal",
    ) -> None:
        """__init__."""
        self.epic = epic
        self.flowsheet_id = flowsheet_id
        self.flowsheet_template_id = flowsheet_template_id
        self.flowsheet_template_id_type = flowsheet_template_id_type
        self.uri = uri

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {
            "epic": self.epic,
            "flowsheet_id": self.flowsheet_id,
            "flowsheet_template_id": self.flowsheet_template_id,
            "flowsheet_template_id_type": self.flowsheet_template_id_type,
            "uri": self.uri,
        }

    def listen(self, listen):
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
                    self.on_notify(listen.notifies.pop())

    def on_notify(self, event):  # pylint: disable=no-self-use
        """On postgres notify handler."""
        logger.debug(
            "NOTIFY: %(pid)s.%(channel)s.%(payload)s",
            {
                "channel": event.chennel,
                "payload": event.payload,
                "pid": event.pid,
            },
        )
        raise NotImplementedError()


class Notification(FlowsheetEgress):
    """Notification Service."""

    YAML = "!notification"

    QUERY = "".join(
        (
            "?",
            "&".join(
                (
                    "PatientID=%(empi)s",
                    "PatientIDType=%(patient_id_type)s",
                    "ContactID=%(csn)s",
                    "ContactIDType=%(contact_id_type)s",
                    "UserID=%(user_id)s",
                    "UserIDType=%(user_type_id)s",
                    "FlowsheetID=%(flowsheet_id)s",
                    "FlowsheetIDType=%(flowsheet_id_type)s",
                    "Value=%(score)s",
                    "Comment=%(comment)s",
                    "InstantValueTaken=%(instant_value_taken)s",
                    "FlowsheetTemplateID=%(flowsheet_template_id)s",
                    "FlowsheetTemplateIDType=%(flowsheet_template_id_type)s",
                )
            ),
        )
    )

    def __init__(
        self,
        *,
        uri: str,
        epic: Epic,
        comment: str = "Not for clinical use.",
        contact_type_id: str = "csn",
        patient_id_type: str = "uid",
        query: str = QUERY,
        **kwargs,
    ) -> None:
        """__init__."""

        super().__init__(
            epic=epic,
            uri=uri + query,
            **kwargs,
        )
        self.comment = comment
        self.contact_type_id = contact_type_id
        self.patient_id_type = patient_id_type

    def __call__(self):
        """__call__."""
        postgres = self.epic.postgres
        sql = postgres.sql
        with postgres.listen(sql.prediction.listen) as listen:
            with postgres.commit() as cur:
                self.recover(cur)
            self.listen(listen)

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {
            "comment": self.comment,
            "contact_type_id": self.contact_type_id,
            "patient_id_type": self.patient_id_type,
            **super().as_yaml(),
        }

    def recover(self, cur):
        """Recover."""
        sql = self.epic.postgres.sql
        cur.execute(sql.epic.notification.recover)
        for each in cur.fetchall():
            self.call_uri(each, cur)

    def call_uri(self, prediction, cur):
        """Call uri."""
        epic = self.epic
        sql = epic.postgres.sql
        uri = self.uri % {
            "csn": prediction["csn"],
            "empi": prediction["empi"],
            "score": prediction["score"],
        }
        request = Request(uri, data=None)
        with urlopen(request, epic.uri_timeout) as response:
            if response.ok:
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


class Verification(FlowsheetEgress):
    """Verification Service."""

    YAML = "!verification"

    def __init__(
        self,
        *,
        epic: Epic,
        uri: str,
        flowsheet_id: str = "3040015333",
        **kwargs,
    ) -> None:
        """__init__."""
        super().__init__(
            epic=epic,
            flowsheet_id=flowsheet_id,
            uri=uri,
            **kwargs,
        )

    def __call__(self):
        """__call__."""
        postgres = self.epic.postgres
        sql = postgres.sql
        with postgres.listen(sql.notification.listen) as listen:
            with postgres.commit() as cur:
                self.recover(cur)
            self.listen(listen)

    def recover(self, cur):
        """Recover."""
        sql = self.epic.postgres.sql
        cur.execute(sql.epic.verification.recover)
        for each in cur.fetchall():
            self.call_uri(each, cur)

    def call_uri(self, notification, cur):
        """Call uri."""
        epic = self.epic
        sql = epic.postgres.sql
        # TODO add notification flowsheet ids to data?
        request = Request(self.uri, data=None)
        with urlopen(request, epic.uri_timeout) as response:
            if response.ok:
                cur.execute(
                    sql.epic.verification.insert,
                    {"notification_id": notification.id},
                )
            else:
                cur.execute(
                    sql.epic.verification_error.insert,
                    {
                        "description": response.text,
                        "name": response.reason,
                        "notification_id": notification.id,
                    },
                )
