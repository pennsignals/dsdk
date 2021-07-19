# -*- coding: utf-8 -*-
"""Epic microservices."""

from logging import getLogger
from select import select
from urllib.request import Request, urlopen

logger = getLogger(__name__)


class Abstract:
    """Abstract Service."""

    def __init__(self, postgres, uri, poll_timeout=60, uri_timeout=5) -> None:
        """__init__."""
        self.postgres = postgres
        self.uri = uri
        self.poll_timeout = poll_timeout
        self.uri_timeout = uri_timeout

    def listen(self, listen):
        """Listen."""
        while True:
            readers, _, exceptions = select(
                [listen], [], [], self.poll_timeout
            )
            if listen.poll():
                while listen.notifies:
                    self.on_notify(listen.notifies.pop())


class Notification(Abstract):
    """Notification Service."""

    URI = "?".join(
        (
            "api/epic/2011/Clinical/Patient/AddFlowsheetValue/FlowsheetValue?",
            "&".join(
                (
                    "PatientID=%(empi)s",
                    "PatientIDType={patient_id_type}",
                    "ContactID=%(csn)s",
                    "ContactIDType={contact_id_type}",
                    "UserID=PENNSIGNALS",
                    "UserIDType=EXTERNAL",
                    "FlowsheetID={flowsheet_id}",
                    "FlowsheetIDType={flowsheet_id_type}",
                    "Value=%(score)s",
                    "Comment={comment}",
                    "InstantValueTaken={instant_value_taken}",
                    "FlowsheetTemplateID={flowsheet_template_id}",
                    "FlowsheetTemplateIDType={flowsheet_template_id_type}",
                )
            ),
        )
    )

    def __init__(
        self, postgres, uri=URI, poll_timeout=60, uri_timeout=5
    ) -> None:
        """__init__."""
        super().__init__(postgres, uri, poll_timeout, uri_timeout)

    def __call__(self):
        """__call__."""
        postgres = self.postgres
        sql = postgres.sql
        with postgres.listen(sql.prediction.listen) as listen:
            with postgres.commit() as cur:
                self.recover(cur)
            self.listen(listen)

    def recover(self, cur):
        """Recover."""
        sql = self.postgres.sql
        cur.execute(sql.epic.notification.recover)
        for each in cur.fetchall():
            self.call_uri(each, cur)

    def on_notify(self, notify):
        """On notify."""
        logger.debug(f"NOTIFY: {notify.pid}.{notify.channel}.{notify.payload}")

    def call_uri(self, prediction, cur):
        """Call uri."""
        sql = self.postgres.sql
        uri = self.uri % {
            "csn": prediction["csn"],
            "empi": prediction["empi"],
            "score": prediction["score"],
        }
        request = Request(uri, data=None)
        with urlopen(request, self.timeout) as response:
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


class Verification(Abstract):
    """Verification Service."""

    URI = "api/epic/2014/Clinical/Patient/GetFlowsheetRows/FlowsheetRows"

    @classmethod
    def main(cls):
        """__main__."""
        pass

    def __init__(
        self, postgres, uri=URI, poll_timeout=60, uri_timeout=5
    ) -> None:
        """__init__."""
        super().__init__(postgres, uri, poll_timeout, uri_timeout)

    def __call__(self):
        """__call__."""
        postgres = self.postgres
        sql = postgres.sql
        with postgres.listen(sql.notification.listen) as listen:
            with postgres.commit() as cur:
                self.recover(cur)
            self.listen(listen)

    def recover(self, cur):
        """Recover."""
        sql = self.postgres.sql
        cur.execute(sql.epic.verification.recover)
        for each in cur.fetchall():
            self.call_uri(each, cur)

    def on_notify(self, notify):
        """On notify."""
        logger.debug(f"NOTIFY: {notify.pid}.{notify.channel}.{notify.payload}")

    def call_uri(self, notification, cur):
        """Call uri."""
        sql = self.postgres.sql
        # TODO add notification flowsheet ids to data?
        request = Request(self.URI, data=None)
        with urlopen(request, self.timeout) as response:
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
