from dataclasses import dataclass
from typing import Optional

from pyjabber.stream.JID import JID

@dataclass(frozen=True, slots=True)
class PendingMessageWrapper:
    """
    Represents messages that could not be sent at the time of the request.

    This includes messages for local clients who are currently disconnected,
    as well as messages destined for entities on external servers that not have
    an open connection.
    """
    jid: JID
    payload: bytes
    external_host: Optional[str] = None
    external_component: Optional[str] = None

    @property
    def is_external(self):
        """
        Indicates whether this message is for an external protocols or not.

        Returns:
            bool: True if this message is for an external protocols or not.
        """
        return self.external_host is not None
    
    @property
    def is_external_component(self):
        """
        Indicate whether this message is for an external component or not.

        Returns:
            bool: True if this message is for an external component
        """
        # TODO: maybe this could be merged with is_external, if external_host is
        # not none and external_host is a subdomain of the server's host
        return self.external_component is not None
