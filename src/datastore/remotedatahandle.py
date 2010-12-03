from scrpc import SCProxy

class RemoteDataHandle(object):
    class ResolutionError(Exception):
        def __init__(self, *args, **kwargs):
            super(RemoteDataHandle.ResolutionError, self).__init__(*args, **kwargs)
        
    def __init__(self, server_address, data_id, size):
        """
        Constructor.
        @type server_address: str or (str, int)-tuple
        @param server_address: The address of the server holding the data.
        If only a single string is given a resolver is needed to do name
        resolution, otherwise an (IP-address, port) tuple is expected.
        @type data_id: int
        @param data_id: The unique id of the data object.
        """
        super(RemoteDataHandle, self).__init__()
        self._server_address = server_address
        self._data_id = data_id
        self._size = size
        self.retain = False

    def _get_size(self):
        return self._size
    size = property(_get_size)

    def _get_server_address(self):
        return self._server_address
    server_address = property(_get_server_address)

    def __len__(self):
        return self.size

    def _get_proxy(self, connection, resolver):
        # If a connection is already given we need not resolve nor connect.
        proxy = None
        if connection != None:
            proxy = connection
        else:
            # We need to make a connection to the server.
            # Check whether name resolution is needed.
            address = None
            if type(self._server_address) == str:
                try:
                    address = resolver.resolve(self._server_address)
                except:
                    raise RemoteDataHandle.ResolutionError("Error resolving server address in remote data handle (%s)."%self._server_address)
            else:
                address = self._server_address
            proxy = SCProxy(address)
        return proxy

    def resolve(self, resolver):
        if type(self._server_address) == str:
            self._server_address = resolver.resolve(self._server_address)

    def fetch(self, connection = None, resolver = None):
        """
        Tries to fetch the data from the server.
        @type connection: SCProxy
        @param connection: An (optional) live connection to the server
        holding the data.
        @type resolver: Unknown
        @param resolver: This should be an object containing a 'resolve'
        method capable of translating a string address to a (ip,port) tuple.
        @rtype: Unknown
        @return: The data held by the server.
        @raise KeyError: This exception is raised if the data id is
        unknown to the server.
        @raise RemoteDataHandle.ResolutionError: Raised if the server name can no be 
        resolved using the given resolver.
        @raise MarshallingError, CommunicationError, RemoteError: All
        of these may be raised by the RPC proxy.
        """
        proxy = self._get_proxy(connection, resolver)
        return proxy.resolve_data_handle(self._data_id, self.retain)

    def refresh(self, connection = None, resolver = None):
        """
        Refreshes the data object at the store. This means that 
        the objects life time is extended.
        @raise KeyError: If the data is unknown at the store, e.g., 
        if it has already expired.
        @raise MarshallingError, CommunicationError, RemoteError: All
        of these may be raised by the RPC proxy.
        """
        proxy = self._get_proxy(connection, resolver)
        proxy.retain_data_handle(self._data_id)

    def expire(self, connection = None, resolver = None):
        """
        Expires (deletes) a data object at the remote store.
        @raise KeyError: If the data is unknown at the store, e.g., 
        if it has already expired.
        @raise MarshallingError, CommunicationError, RemoteError: All
        of these may be raised by the RPC proxy.
        """
        proxy = self._get_proxy(connection, resolver)
        proxy.expire_data_handle(self._data_id)
        
