class DataStore:
    exception = RuntimeError

    def get_identifier(self):
        return '-dummy-'

    def package_show(self, id):
        raise NotImplementedError()
    
    def package_search(self, **kwargs):
        raise NotImplementedError()

    def resource_search(self, query):
        raise NotImplementedError()

    def recently_changed_packages_activity_list(self):
        raise NotImplementedError()

class CkanDataStore(DataStore):
    """
    connects to CKAN client to use the APIs to return packages from the ckan instance
    """
    def __init__(self, url):
        self.url = url
        self.client = self.make_client(url)

        from ckanapi.errors import CKANAPIError
        self.exception = CKANAPIError
        # any exception raised via the object throws a ckanapi error

    def get_identifier(self):
        return self.url

    @classmethod
    def make_client(cls, url):
        """
        connects to the lintol ckan server
        """
        from ckanapi import RemoteCKAN
        return RemoteCKAN(url, user_agent='lintol-doorstep-crawl/1.0 (+http://lintol.io)')

    def package_show(self, id):
        # uses ckan api to show a package's metadata
        resources = self.client.action.package_show(id=id)
        return resources

    def resource_search(self, query):
        # queries the ckan instance
        resources = self.client.action.resource_search(query=query)
        return resources

    def package_search(self, **kwargs):
        package_search = self.client.action.package_search(**kwargs)
        return package_search

    def package_list(self):
        package_list = self.client.action.package_list()
        return package_list

    def recently_changed_packages_activity_list(self):
        # api to get 50 most recently changed packages
        recently_changed = self.client.action.recently_changed_packages_activity_list()
        return recently_changed

class DummyDataStore(DataStore):
    """
    object to return dummy data.
    could be used for testing
    """
    def package_show(self, id):
        # returns resources for each package
        # ie, one package could have 4 datasets associated with it
        resources = {'resources' : self.resource_search('')}
        return resources

    def package_search(self, **kwargs):
        package_search = {'results' :
                            ['value 1', 'value 2', 'value 3', 'value 4']
                        }
        return package_search

    def resource_search(self, query):
        # returns list of dictionaries, which mimicks what is returned from the ckan server
        resources = [{'url': 'dataset1', 'created': '1558714354.245544'},
                        {'url': 'dataset2', 'created': '1095379199.50'},
                        {'url': 'dataset3', 'created': '1558714354.245544'},
                        {'url': 'dataset4', 'created': '1095379200.25'},
                        {'url': 'dataset5', 'created': '1558714405.460863'},
                        {'url': 'dataset6', 'created': '1095379200.25'},
                        {'url': 'dataset7', 'created': '1095379199.25'},
                        {'url': 'dataset8', 'created': '1095379199.50'}]
        return resources

    def recently_changed_packages_activity_list(self):
        # returns list of dictionaries, which mimicks what is returned from the ckan server
        recently_changed = [
                {
                    'revision_id': '1'
                }, {
                    'revision_id': '2'
                }, {
                    'revision_id': '3'
                }, {
                    'revision_id': '5'
                }, {
                    'revision_id': '4'
                }
        ]
        return recently_changed
