class NewConnectionWrapper:
    """
    Represents a new connection made.
    It can be from a client, an external component or a server.
    """
    __slots__ = ('value', 'client', 'component')
    def __init__(self, value, client=True, component=False):
        self.value = value
        self.client = client
        self.component = component
