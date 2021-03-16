import datetime
import time
import requests
import six
from six.moves import urllib
from six import string_types
from urllib.parse import urljoin
from functools import wraps
import collections
from datamodel.webservice import Struct


def urljoiner(baseurl, path_or_pathlist):
    if isinstance(path_or_pathlist, string_types):
        if(baseurl.rsplit('/', 1)[-1] == "rest"):
            return urljoin(baseurl, "rest" + path_or_pathlist)
        else:
            return urljoin(baseurl, path_or_pathlist)
    else:
        if(baseurl.rsplit('/', 1)[-1] == "rest"):
            return urljoin(
                baseurl, (('/'.join(["rest"] + path_or_pathlist))).replace("//", "/"))
        else:
            return urljoin(baseurl, '/'.join(path_or_pathlist))

class ComputeTypes(object):
    AsStored = 'AsStored'
    MissingOnly = 'MissingOnly'
    All = 'All'
    
class AggregateTypes(object):
    OnCompletion = 'OnCompletion'
    DoNotProcess  = 'DoNotProcess'

class AnalysisOptions(Struct):
    def __init__(self, risk_data_source="Default",
                       data_partition="AxiomaUS",
                       compute=ComputeTypes.MissingOnly,
                       aggregate=AggregateTypes.OnCompletion, *args, **kw):
                
        struct_dict = dict(riskDataSource=risk_data_source,
                           dataPartition=data_partition,
                           aggregationOptions=dict(compute=compute,aggregate=aggregate))
        struct_dict.update(locals()['kw'])
        super().__init__(struct_dict)

def RefreshHandler(cls):
    """
    Class decorator to keep the instance of AxRiskConnector
    always alive and useful.
    """
    def relogin(func):
        @wraps(func)
        def wrapped_func(self, *args, **kwargs):
            if self.is_time_to_refresh():
                self.login()
            ret = func(self, *args, **kwargs)
            return ret
        return wrapped_func

    methods = [a for a in cls.__dict__ if not a.endswith('__')  and a not in ('login','is_time_to_refresh', 'handle_error_message') and isinstance(getattr(cls,a), collections.Callable)]
    for method in methods:
        setattr(cls, method, relogin(getattr(cls, method)))
    return cls

class AxRiskConnector:
    def __init__(self,
                 host="",
                 port=None,
                 path="api/v1",
                 user="",
                 passwd="",
                 grant_type="password",
                 client_id="",
                 debug=False,
                 protocol='http'):

        if(port is None and protocol == "http"):
            port = str(8681)
            self.baseurl = '%s://%s:%s' % (protocol, host, port)
        elif(port is None and protocol == "https"):
            self.baseurl = '%s://%s' % (protocol, host)

        self.path = path
        self.user = user
        self.debug = int(debug)
        self.last_request = None

        self.auth_data = {'username': user,
                          'password': passwd,
                          'client_id': client_id,
                          'grant_type': grant_type}

        self.login()

    def handle_error_message(self, output):
        self.last_request = output
        if(self.debug):
            print(str(output.status_code) + "... Total seconds taken: " +
                  str(output.elapsed.total_seconds()))
        if(output.status_code > 399):
            print("Error in Axioma Risk Connector")
            print("Status code: " + str(output.status_code))
            print("Problem: " + output.content.decode("utf-8"))
            print("Reason: " + output.reason)
            raise Exception(
                "Exiting because: " +
                output.content.decode("utf-8"))

    def login(self):
        """
        login method to api
        """
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        token_url = urljoiner(self.baseurl, ["connect/token"])
        if(self.debug):
            print(token_url)
            new_auth = dict(self.auth_data)
            new_auth['password'] = "XXXXX"
            print("Authentication Data (without password):")
            print(new_auth)
        r = requests.post(token_url, data=self.auth_data, headers=headers)
        # New environments do not redirect /rest/connect/token to
        # /auth/connect/token so lets check this case explicitly
        if(r.status_code > 400):
            new_token_url = self.baseurl.rstrip(
                "/rest") + "/auth/connect/token"
            if(self.debug):
                print("cannot connect to: " + token_url)
                print("trying: " + new_token_url)
            r = requests.post(
                new_token_url,
                data=self.auth_data,
                headers=headers)
        self.last_login = time.time()
        self.handle_error_message(r)
        self.auth_result = r.json()
        access_token = r.json().get('access_token')
        self.headers = {'Authorization': 'Bearer ' + access_token,
                        'Content-Type': 'application/json'}
        # Always relogin when time remaining on the current token is in between 1 min and 3 min
        self.refresh_window = min(max(60, 0.01 * self.auth_result['expires_in']), 180)
    
    def is_time_to_refresh(self):
        elasped_time_since_last_login = time.time() - self.last_login
        if elasped_time_since_last_login > self.auth_result['expires_in'] - self.refresh_window:
           return True
        return False

    def get_href_url(self, href):
        """Endpoint to perform a GET from a Axioma Risk URL (e.g.)
        
        Endpoint:
            {baseurl} + href
            
        Keyword arguments:
            url: str or dict with 'href' key 
                Example 1. '/api/v1/templates/*/*/Present%20Value/3615270'
                Example 2. {'href':'/api/v1/templates/*/*/Present%20Value/3615270'}
        """
        verb = "GET"
        if(isinstance(href,str)):
            url = urljoiner(self.baseurl, [href])
        elif(isinstance(href,dict) and 'href' in href): # check if dictionary and has key
            url = urljoiner(self.baseurl, [href['href']])
        if(self.debug):
            print(verb + " " + url)
        r = requests.get(url, headers=self.headers)
        self.handle_error_message(r)
        return r.json()
        
    def reset_password(self, old_password, new_password):
        """
        You need to be admin
        for resetting password
        """
        verb = "POST"
        url = urljoiner(self.baseurl, [self.path, "$me", "password"])
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {"oldPassword": old_password,
                "newPassword": new_password}

        if(self.debug):
            print(verb + " " + url)
        r = requests.post(url, data=data, headers=headers)
        self.handle_error_message(r)
        print("password successfully reset!")
        self.auth_data['password'] = new_password
        try:
            self.login()
        except Exception as e:
            pass

    def __convert_datetime_to_string__(self, date):
        if(isinstance(date, string_types)):
            from dateutil.parser import parse
            return self.__convert_datetime_to_string__(parse(date))
        else:
            return date.strftime("%Y-%m-%d")

    def get_team_names(self):
        """Lists Teams. (Auth)

        Endpoint:
            /admin/teams

        Keyword arguments:
            None

        """
        verb = "GET"
        url = urljoiner(self.baseurl, [self.path, "admin", "teams"])
        if(self.debug):
            print(verb + " " + url)
        r = requests.get(url, headers=self.headers)
        self.handle_error_message(r)
        return r.json()

    def get_user_names(self):
        """Lists Users. (Auth)

        Endpoint:
            /admin/users

        Keyword arguments:
            None

        """
        verb = "GET"
        url = urljoiner(self.baseurl, [self.path, "admin", "users"])
        if(self.debug):
            print(verb + " " + url)
        r = requests.get(url, headers=self.headers)
        self.handle_error_message(r)
        return r.json()

    def get_portfolio_names(self, match_case=""):
        """Lists Portfolios. (Auth policies: Users)

        Endpoint:
            /portfolios

        Keyword arguments:
            match_case: passed to Odata filter

        """
        verb = "GET"
        url = urljoiner(
            self.baseurl, [
                self.path, "portfolios?$filter=contains(name, '" + urllib.parse.quote_plus(match_case) + "')"])
        if(self.debug):
            print(verb + " " + url)
        r = requests.get(url, headers=self.headers)
        self.handle_error_message(r)
        return r.json()

    def get_portfolio_id(self, portfolio_name):
        pIds = self.get_portfolio_names(match_case=portfolio_name)
        portfolio_names = {i['id']: i['name'] for i in pIds['items']}
        names = []
        for key, value in six.iteritems(portfolio_names):
            if portfolio_name == value:
                names.append(key)
        if(len(names) == 0):
            raise LookupError("Error... cannot find portfolio name " +
                              portfolio_name)
        elif(len(names) > 1):
            raise LookupError("Error... multiple portfolios with name " +
                              portfolio_name)
        else:
            return names[0]

    def __convert_to_pid__(self, pID):
        if(isinstance(pID, string_types)):
            return self.get_portfolio_id(pID)
        else:
            return pID

    def copy_portfolio(self,
                       portfolio,
                       from_date,
                       to_date,
                       overwrite=False,
                       copy_ref_portfolio=False):
        """Copies a portfolio using the rollover method

        Keyword arguments:
            portfolio_id: portfolio name or id to copy
            from_date: datetime or string of portfolio date
            to_date: datetime or string of portfolio date
            overwrite: bool (default: False)  whether the undelylying portfolio should be deleted first
            copy_ref_portfolio: bool (default: False) if true we copy and the portfolio references another portfolio we copy that too

        """
        verb = "POST"
        portfolio_id = self.__convert_to_pid__(portfolio)
        if(overwrite):
            if(to_date in self.get_position_date_info(portfolio_id).keys()):
                self.delete_positions_for_date(portfolio_id, to_date)
        data = {
            "rollOverToDate": str(to_date) + "T00:00:00",
            "attributes": {
                "IsRollOver": "true"
            }
        }
        url = urljoiner(self.baseurl, [self.path,
                                       "portfolios",
                                       str(portfolio_id),
                                       "positions",
                                       str(from_date),
                                       "rollover-requests"])
        if(self.debug):
            print(verb + " " + url)
            print(data)
        r = requests.post(url, json=data, headers=self.headers)
        if(copy_ref_portfolio):
            folio = self.get_positions_for_date(portfolio_id, from_date)
            for position in folio:
                if(position['identifiers'][0]['type'].lower() == "portfolio"):
                    self.copy_portfolio(
                        position['identifiers'][0]['value'], from_date, to_date)

        self.handle_error_message(r)
        return True

    def delete_positions_for_date(self, portfolio, date):
        """Deletes all Positions from the Portfolio for a given date (Auth policies: Users)

        Endpoint:
            /portfolios/{id}/positions/{date}

        Keyword arguments:
            portfolio: portfolio name or id
            date: date to delete

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        verb = 'DELETE'
        url = urljoiner(self.baseurl, [self.path,
                                                          'portfolios',
                                                          str(portfolio_id),
                                                          "positions",
                                                          str(date)])
        if(self.debug):
            print(verb + ' ' + url)
        r = requests.delete(url,
                            headers=self.headers)
        self.handle_error_message(r)
        return True

    def get_portfolio(self, portfolio):
        """Returns a single portfolio. (Auth policies: Users)

        Endpoint:
            /portfolios/{id}

        Keyword arguments:
            portfolio: portfolio name or id

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        portfolio_position_url = urljoiner(self.baseurl, [self.path,
                                                          'portfolios',
                                                          str(portfolio_id)])
        if(self.debug):
            print(portfolio_position_url)
        r = requests.get(portfolio_position_url,
                         headers=self.headers)
        self.handle_error_message(r)
        return r.json()

    def get_portfolio_benchmark(self, portfolio):
        """Returns a portfolio benchmark. (Auth policies: Users)

        Endpoint:
            /portfolios/{id}/benchmark

        Keyword arguments:
            portfolio: portfolio name or id

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        portfolio_position_url = urljoiner(self.baseurl, [self.path,
                                                          'portfolios',
                                                          str(portfolio_id),
                                                          "benchmark"])
        if(self.debug):
            print(portfolio_position_url)
        r = requests.get(portfolio_position_url,
                         headers=self.headers)
        self.handle_error_message(r)
        return r.json()    
        
    def get_portfolio_valuations(self, portfolio):
        """Lists dates there are Valuations for the Portfolio, latest first (Auth policies: Users)

        Endpoint:
            /api/v1/portfolios/{id}/valuations

        Keyword arguments:
            portfolio: portfolio name or id

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        portfolio_position_url = urljoiner(self.baseurl, [self.path,
                                                          'portfolios',
                                                          str(portfolio_id),
                                                          "valuations"])
        if(self.debug):
            print(portfolio_position_url)
        r = requests.get(portfolio_position_url,
                         headers=self.headers)
        self.handle_error_message(r)
        return r.json()

    def get_portfolio_valuations_for_date(self, portfolio, date):
        """Retrieves portfolio valuation for a given id+date (Auth policies: Users)

        Endpoint:
            /api/v1/portfolios/{id}/valuations/{date}

        Keyword arguments:
            portfolio: portfolio name or id
            date: date for valuation

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        portfolio_position_url = urljoiner(self.baseurl, [self.path,
                                                          'portfolios',
                                                          str(portfolio_id),
                                                          "valuations",
                                                          str(date)])
        if(self.debug):
            print(portfolio_position_url)
        r = requests.get(portfolio_position_url,
                         headers=self.headers)
        self.handle_error_message(r)
        return r.json()

    def save_portfolio_valuations_for_date_put(
            self, portfolio, date, valuation):
        """Replaces or creates the portfolio valuation for the given id+date (Auth policies: Users)

        Endpoint:
            /api/v1/portfolios/{id}/valuations/{date}

        Keyword arguments:
            portfolio: portfolio name or id
            date: date for valuation
            valuation:

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        portfolio_position_url = urljoiner(self.baseurl, [self.path,
                                                          'portfolios',
                                                          str(portfolio_id),
                                                          "valuations",
                                                          str(date)])
        if(self.debug):
            print(portfolio_position_url)
        r = requests.put(portfolio_position_url,
                         json=valuation,
                         headers=self.headers)
        self.handle_error_message(r)
        return True

    def get_position_date_info(self, portfolio_id):
        """Lists dates there are Positions for the Portfolio, latest first (Auth policies: Users)
        
        Endpoint:
            /portfolios/{id}/positions

        Keyword arguments:
            portfolio_id: portfolio name or id
        """
        portfolio_id = self.__convert_to_pid__(portfolio_id)
        
        verb = 'GET'
        url = urljoiner(self.baseurl, [self.path,
                                                          'portfolios',
                                                          str(portfolio_id),
                                                          'positions'])
        if(self.debug):
            print(verb + ' ' + url)
        r = requests.get(url,
                         headers=self.headers)
        self.handle_error_message(r)

        position_info = r.json().get("items")
        # Put the position date and position Count in a dict
        date_position_info = {}
        for posInfo in position_info:
            date_position_info[posInfo.get(
                "asOfDate")] = posInfo.get("positionsCount")
        return date_position_info

    def get_latest_portfolio_date(self, portfolio_id):
        return (self.get_portfolio(portfolio_id))['latestPositionDate']

    def get_positions_for_date(self, portfolio, position_date=None):
        """Lists Positions on the given date for the Portfolio (Auth policies: Users)

        Endpoint:
            /portfolios/{id}/positions/{date}

        Keyword arguments:
            portfolio: portfolio name or id
            position_date: date to grab portfolio (default=latestPositionDate)

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        if(position_date is None):
            position_date = self.get_latest_portfolio_date(portfolio_id)
        if isinstance(position_date, (datetime.date, datetime.datetime)):
            position_date = position_date.strftime('%Y-%m-%d')
        portfolio_positions_url = urljoiner(self.baseurl, [self.path,
                                                           'portfolios',
                                                           str(portfolio_id),
                                                           'positions',
                                                           position_date])
        if(self.debug):
            print(portfolio_positions_url)
        r = requests.get(portfolio_positions_url,
                         headers=self.headers)
        self.handle_error_message(r)
        position_dates_json = r.json()
        return position_dates_json.get("items")

    def save_positions_for_date_post(self,
                                     portfolio,
                                     date,
                                     position_data):
        """Create a new Position in the Portfolio on the given date. (Auth policies: Users)

        Endpoint:
            /portfolios/{id}/positions/{date}

        Keyword arguments:
            portfolio: portfolio name or id
            date: date to save position
            position_data: position to save

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        position_date = self.__convert_datetime_to_string__(date)
        portfolio_positions_url = urljoiner(self.baseurl, [self.path,
                                                           'portfolios',
                                                           str(portfolio_id),
                                                           'positions',
                                                           position_date])
        if(self.debug):
            print(portfolio_positions_url)
            print(position_data)
        r = requests.post(portfolio_positions_url,
                          json=position_data,
                          headers=self.headers)
        self.handle_error_message(r)
        return r

    def save_positions_for_date_patch(self,
                                      portfolio,
                                      date,
                                      position_data):
        """Patches the existing collection of Positions according to the supplied operations (Auth policies: Users)

        Endpoint:
            /portfolios/{id}/positions/{date}

        Keyword arguments:
            portfolio: portfolio name or id
            date: date to save position
            position_data: list of positions to save

        """
        portfolio_id = self.__convert_to_pid__(portfolio)
        position_date = self.__convert_datetime_to_string__(date)
        
        verb = 'PATCH'
        url = urljoiner(self.baseurl, [self.path,
                                                           'portfolios',
                                                           str(portfolio_id),
                                                           'positions',
                                                           position_date])
        if(self.debug):
            print(verb + ' ' + url)
        
        if 'upsert' not in position_data:
            data = {'upsert': position_data, 'remove': []}
        else:
            data = position_data
            
        r = requests.patch(url,
                           json=data,
                           headers=self.headers)
        self.handle_error_message(r)
        return r

    def create_portfolio(self, name, args={}):
        """Create a new portfolio (Auth policies: Users)

        Endpoint:
            /portfolios

        Keyword arguments:
            name: portfolio name
            args(optional): arguments to pass to post
        """
        verb = "POST"
        url = urljoiner(self.baseurl, [self.path, 'portfolios'])
        args['name'] = name
        if(self.debug):
            print(verb + " " + url)
            print(args)
        r = requests.post(url,
                          json=args,
                          headers=self.headers)
        self.handle_error_message(r)
        return r.headers

    def update_portfolio(self, portfolio, data):
        """Updates a portfolio (Auth policies: Users)

        Endpoint:
            /portfolios/{id}

        Keyword arguments:
            portfolio: portfolio to delete
            data: json to update portfolio
        """
        verb = "PUT"
        portfolio_id = self.__convert_to_pid__(portfolio)
        url = urljoiner(self.baseurl, [self.path, 'portfolios', str(portfolio_id)])
        if(self.debug):
            print(verb + " " + url)
        r = requests.put(url,
                          json=data,
                          headers=self.headers)
        self.handle_error_message(r)
        return r.headers

    def delete_portfolio(self, portfolio):
        """Deletes a portfolio (Auth policies: Users)

        Endpoint:
            /portfolios/{id}

        Keyword arguments:
            portfolio: portfolio to delete

        """
        verb = "DELETE"
        portfolio_id = self.__convert_to_pid__(portfolio)
        url = urljoiner(self.baseurl, [self.path,
                                       'portfolios',
                                       str(portfolio_id)])
        if(self.debug):
            print(verb + " " + url)
        r = requests.delete(url,
                            headers=self.headers)
        self.handle_error_message(r)
        return True
RetryAxRiskConnector = RefreshHandler(AxRiskConnector)

