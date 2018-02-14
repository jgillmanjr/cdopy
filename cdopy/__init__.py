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
    def __call__(self, endpoint, item=None, stash_data=True, **kwargs):
        """
        Return a result object
        """
        return Result(token=self.token, api_version=self.api_version, endpoint=endpoint, item=item,
                      stash_data=stash_data, **kwargs)

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

    def __init__(self, token, api_version, endpoint, item, stash_data, **kwargs):
        # Call the parent constructor
        super().__init__(token=token, api_version=api_version)
        self.stash_data = stash_data  # If we want to store the results locally as they roll in
        self.stashed_data = []

        # This is so we can be smart and use next with the generator..
        self._last_fetched_set = []

        # Init the location stuff, even if not used
        self._fetch_status = {
            'offset': 1,  # For their API, 0 is also treated as 1...
            'more_pages': True,  # Is there another page?
            'start_use_stashed': False  # Basically so we don't double dump off the bat
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
        for k, v in kwargs.items():
            self.request_params['params'][k] = v
        if item is not None:
            self.single_item = True
            self.request_params['url'] += '/' + str(item)

        # Make the initial request
        self._request()

    def _request(self):
        if self._fetch_status['more_pages'] and len(self._last_fetched_set) == 0:
            self._latest_response = requests.get(**self.request_params)
            if self._latest_response.status_code == requests.codes.ok:
                return_dict = self._latest_response.json()
                if 'metadata' in return_dict:  # Dealing with a collection
                    count = return_dict['metadata']['resultset']['count']
                    limit = self.request_params['params']['limit']  # Use this just in case some schmuck changes things..
                    offset = return_dict['metadata']['resultset']['offset']

                    # Prep the next run if it happens
                    self._fetch_status['offset'] = offset + limit
                    self.request_params['params']['offset'] = offset + limit

                    if (limit + offset) > count:
                        # Nothing more to see here, folks
                        self._fetch_status['more_pages'] = False

                    self._last_fetched_set = return_dict['results']
                    return None

                # Single Item
                self._fetch_status['more_pages'] = False
                self._last_fetched_set = [return_dict]
                return None

            else:
                pprint(self._latest_response.request.headers)
                pprint(self._latest_response.headers)
                pprint(self._latest_response.json())
                self._latest_response.raise_for_status()

    def results(self):
        while len(self._last_fetched_set) > 0:
            result = self._last_fetched_set.pop(0)
            if self.stash_data:
                self.stashed_data.append(result)
            if len(self._last_fetched_set) == 0:
                self._request()
            yield result
