import requests
from pprint import pprint

BASE_URI = 'https://www.ncdc.noaa.gov/cdo-web/api/'
VERSION = 'v2'

ENDPOINTS = {
    'datasets': 'A dataset is the primary grouping for data at NCDC.',
    'datacategories': 'A data category is a general type of data used to group similar data types.',
    'datatypes': 'A data type is a specific type of data that is often unique to a dataset.',
    'locationcategories': 'A location category is a grouping of similar locations.',
    'locations': 'A location is a geopolitical entity.',
    'stations': 'A station is a any weather observing platform where data is recorded.',
    'data': 'A datum is an observed value along with any ancillary attributes at a specific place and time.',
}


class Client:
    """
    Base Client
    """
    def __call__(self, endpoint, item=None, **kwargs):
        """
        Return a result object
        """
        return Result(token=self.token, api_version=self.api_version, endpoint=endpoint, item=item, **kwargs)

    def __init__(self, token, api_version=VERSION):
        self.token = token
        self.api_version = api_version
        self.base_uri = BASE_URI + self.api_version


class Result(Client):
    """
    A Result
    """
    def __call__(self, **kwargs):
        pass

    def __init__(self, token, api_version, endpoint, item, **kwargs):
        # Call the parent constructor
        super().__init__(token=token, api_version=api_version)

        # Init the location stuff, even if not used
        self.location = {
            'offset': 0,
            'count': 0,
        }

        # Build the initial parameters to be sent for the request
        self.request_params = {
            'url': self.base_uri + '/' + endpoint,
            'headers': {
                'token': self.token
            },
            'params': {
                'limit': 1000,  # Default to the max, because why not
            },
        }
        for k, v in kwargs:
            self.request_params['params'][k] = v
        if item is not None:
            self.single_item = True
            self.request_params['url'] += '/' + str(item)

        # Instantiate for visibility
        self.latest_results = None

        # If the request is for a single item, just get it now
        if self.single_item:
            #self.request_params['params'] = None  # Apparently they don't like this
            self.latest_results = self._request()

    def _request(self):
        self._latest_response = requests.get(**self.request_params)
        if self._latest_response.status_code == requests.codes.ok:
            return_dict = self._latest_response.json()
            if 'metadata' in return_dict:  # Dealing with a collection
                pass
            return [return_dict]
        else:
            pprint(self._latest_response.request.headers)
            pprint(self._latest_response.headers)
            pprint(self._latest_response.json())
            self._latest_response.raise_for_status()
