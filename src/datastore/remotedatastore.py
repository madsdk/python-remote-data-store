from __future__ import with_statement
from remotedatahandle import RemoteDataHandle
from thread import allocate_lock
from uuid import uuid4
from time import time
from cPickle import dump, load
import os

class DataStoreError(Exception):
    def __init__(self, *args, **kwargs):
        super(DataStoreError, self).__init__(*args, **kwargs)
        
class DataObject(object):
    def __init__(self, data_id, filename, timestamp):
        super(DataObject, self).__init__()
        self.data_id = data_id
        self.filename = filename
        self.timestamp = timestamp
        
class RemoteDataStore(object):
    def __init__(self, store_address, path = '', timeout=300):
        """
        Constructor.
        @type path: str
        @param path: The base path of the storage area. In this folder
        a subfolder named datastore will be created.
        @type timeout: int
        @param timeout: The life time of data stored in the data store.
        After this amount of seconds a data item will be removed from
        the local disk.
        """
        super(RemoteDataStore, self).__init__()
        self._store_address = store_address
        self._path = os.path.join(path, 'datastore') if path != '' else 'datastore'
        self._timeout = timeout
        self._data_lock = allocate_lock()
        self._data = {}
        self._next_id = 0
        
        try:
            # Check that the path exists - otherwise try to create it.
            if os.path.exists(self._path):
                # Clean the old datastore.
                for root, dirs, files in os.walk(self._path, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
            else:
                os.mkdir(self._path)
        except Exception, e:
            raise DataStoreError('Error initializing data store.', e)

    def store_data(self, data):
        """
        Stores some data on the local file system.
        @type data: anything picklable
        @param data: The data to store.
        @raise PicklingError: If the object is not picklable.
        @rtype: RemoteDataHandle
        @return: A data handle that may be used to fetch the data.
        """
        with self._data_lock:
            # Pick an id for the data.
            data_id = self._next_id
            self._next_id += 1
            
            # Store the data on the local disk.
            filename = uuid4().hex
            path = os.path.join(self._path, filename)
            try:
                with open(path, 'w') as outfile:
                    dump(data, outfile, -1)
            except Exception, e:
                raise DataStoreError('Error writing data into store.', e)
            self._data[data_id] = DataObject(data_id, filename, time())
            
            # Return a data handle.
            return RemoteDataHandle(self._store_address, data_id, os.path.getsize(path))

    def _remove_data_object(self, data_id):
        try:
            data_obj = self._data.pop(data_id)
            os.remove(os.path.join(self._path, data_obj.filename))
        except:
            pass

    def fetch_data(self, data_id, retain = False):
        """
        Fetch and return some data from the store.
        @type data_id: int
        @param data_id: The id of the data object.
        @type retain: bool
        @param retain: Whether or not to retain the data object. If
        retain is True the object will not be deleted immediately 
        after being fetched; in fact its timestamp will be updated
        so that it lives for at least another timeout period.
        @return: The data object.
        @raise KeyError: If the requested data object does not exist.
        """
        # Fetch the data object from disk.
        data_obj = None
        with self._data_lock:
            try:
                data_obj = self._data[data_id]
            except Exception, e:
                raise DataStoreError('Error fetching data.', e)
        
            data = None
            try:
                with open(os.path.join(self._path, data_obj.filename)) as infile:
                    data = load(infile)

                # Remove the data object if not retained.
                if not retain:
                    self._remove_data_object(data_obj.data_id)
                else:
                    # The data object must be retained. Update its timestamp.
                    data_obj.timestamp = time()
                return data
            except Exception, e:
                raise DataStoreError('Error fetching data.', e)

    def retain(self, data_id):
        """
        Ask the data store to hold on to a piece of data for a while longer.
        @type data_id: int
        @param data_id: The id of the data object to retain.
        @raise KeyError: Raised if the given id does not exist.
        """
        with self._data_lock:
            try:
                data_obj = self._data[data_id]
            except Exception, e:
                raise DataStoreError('Error retaining data.', e)
            data_obj.timestamp = time()

    def expire(self, data_id):
        """
        Remove a data object from the store.
        @raise KeyError: Raised if the given id does not exist.
        """
        with self._data_lock:
            try:
                self._remove_data_object(data_id)
            except Exception, e:
                raise DataStoreError('Error expiring data object.', e)

    def resolve_data_handle(self, data_handle, resolver = None):
        """
        This method tries to fetch the data of the given remote data handle.
        The only difference between using this method and simply using the 
        fetch method of the data handle is that this method is capable of
        checking whether the data handle could be resolved locally, i.e., 
        that the data resides within this data store.
        """
        if data_handle.server_address == self._store_address:
            # Local fetch.
            return self.fetch_data(data_handle._data_id, data_handle.retain)
        else:
            # Try to do a remote fetch. Note that there is no resolver here, 
            # so the data handle server address must be a (ip,port) tuple.
            return data_handle.fetch(resolver=resolver)

    def cleanup(self):
        """Clean up method. This removes stale data objects."""
        # Check for stale data objects.
        with self._data_lock:
            now = time()
            for data_obj in self._data.values():
                if now - data_obj.timestamp > self._timeout:
                    # Remove the data object.
                    self._data.pop(data_obj.data_id)
                        # And remove the actual data from the disk.
                    try:
                        os.remove(os.path.join(self._path, data_obj.filename))
                    except:
                        # Silently fail for now...
                        pass
