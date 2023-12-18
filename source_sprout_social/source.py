#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from urllib.parse import parse_qsl, urlparse


# Basic full refresh stream
class SproutSocialStream(HttpStream, ABC):
    """
    Parent class extended by all stream-specific classes
    """

    url_base = "https://api.sproutsocial.com/v1/"

    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.url_base = "https://api.sproutsocial.com/v1/"
        self.page = 1

    def _get_customer_id(self):
        """
        Given an API key, make a request to the ClientMetadata endpoint to return the Customer ID. This is required for all other endpoints.
        ASSUMES ONE CUSTOMER_ID PER API KEY, is that correct @kane? Or does this need to return a list?

        This method can be called in streams that require a customer_id, for example when creating a CustomerProfiles stream:

        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/metadata/customer"

        """

        client_metadata_endpoint = "metadata/client"
        client_metadata_url = self.url_base + client_metadata_endpoint
        headers = {"Authorization": f"Bearer {self.config['api_key']}" }
        customer_id = requests.get(client_metadata_url, headers=headers).json()["data"][0]["customer_id"]
        # if this needs to return a list, then we need to do fancier things with stream slices (like in that Airbyte thread you found)
        # but if we can assume 1 customer ID per API key, then this should work fine?

        return customer_id

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.config['api_key']}" }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Pagination for most endpoints (except messages) is acheived by incrementing `/page/#` in the request URL.
        
        Does the response url contain how many pages there will be? Maybe.
        
        This logic was lifted directly from https://github.com/community-tech-alliance/airbyte-source-twilio/blob/main/source_twilio/streams.py """
        stream_data = response.json()
        next_page_uri = stream_data.get("next_page_uri")
        if next_page_uri:
            next_url = urlparse(next_page_uri)
            next_page_params = dict(parse_qsl(next_url.query))
            return next_page_params

    def request_params(
        self, stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """can probably comment out for prelim testing"""
        # params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        # params["PageSize"] = self.page_size
        # if next_page_token:
        #     params.update(**next_page_token)
        # return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        I think I copied this from another sync... may need work.
        """

        # NOTE FOR KANE: Seems like all the responses from this API put the data in a "data" key, so this method should grab data from that key.
        response_json = response.json()["data"]
        yield from response_json


class ClientMetadata(SproutSocialStream):
    primary_key = "customer_id"

    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        endpoint = "metadata/client"
        return endpoint
    
class CustomerProfiles(SproutSocialStream):
    primary_key = "customer_profile_id"
    """This endpoint retrieves data from the `{customer_id}/metadata/customer` endpoint as a get request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`"""

    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/metadata/customer"
        
        return endpoint
    
class CustomerTags(SproutSocialStream):
    primary_key = "tag_id"
    """This endpoint retrieves data from the `{customer_id}/metadata/customer/tags` endpoint as a get request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`"""

    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/metadata/customer/tags"

        return endpoint
    
class CustomerGroups(SproutSocialStream):
    primary_key = "group_id"
    """This endpoint retrieves data from the `{customer_id}/metadata/customer/groups` endpoint as a get request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`"""


    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/metadata/customer/groups"

        return endpoint
    
class CustomerUsers(SproutSocialStream):
    primary_key = "id"
    """This endpoint retrieves data from the `{customer_id}/metadata/customer/users` endpoint as a get request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`"""

    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/metadata/customer/users"

        return endpoint
    
# class CustomerProfileAnalytics(SproutSocialStream):
#     primary_key = "customer_profile_id"
#     """This endpoint retrieves data from the `analytics/profiles` endpoint as a post request.   
#     The request needs: 
#       - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
#       - a list (but saved as a string) from the `metadata/customer` endpoint based on `network_type` (aka social media site)
#       - a json specifically filtered for each `network_type` (aka social media site) 
#         (in colab notebook, uses `post_api` function and `facebook_analytics_profiles`, `instagram_analytics_profiles`, and `tiktok_analytics_profiles` as json data)       
#      """

#     def path(
#         self, stream_state: Mapping[str, Any] = None, 
#         stream_slice: Mapping[str, Any] = None, 
#         next_page_token: Mapping[str, Any] = None,
#         **kwargs,
#     ) -> str:
#         endpoint = f"{customer_id}/analytics/profiles"
#         return endpoint
    
# class ProfileAnalytics(SproutSocialStream):
#     primary_key = "customer_profile_id"
#     """This endpoint retrieves data from the `analytics/profiles` endpoint as a post request.   
#     The request needs: 
#       - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
#       - a list (but saved as a string) from the `metadata/customer` endpoint based on `network_type` (aka social media site)
#         (in colab notebook these string lists are saved as: `facebook`,`instagram`,`tiktok` and were not created programmatically)       
#       - a json specifically filtered for each `network_type` (aka social media site) 
#         (in colab notebook, uses `post_api` function and `facebook_analytics_profiles`, `instagram_analytics_profiles`, and `tiktok_analytics_profiles` as json data)       
#      """

#     def path(
#         self, stream_state: Mapping[str, Any] = None, 
#         stream_slice: Mapping[str, Any] = None, 
#         next_page_token: Mapping[str, Any] = None,
#         **kwargs,
#     ) -> str:
#         endpoint = f"{customer_id}/analytics/profiles"
#         return endpoint

# class PostAnalytics(SproutSocialStream):
#     primary_key = "customer_profile_id"
#     """This endpoint retrieves data from the `analytics/profiles` endpoint as a post request.   
#     The request needs: 
#       - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
#       - a list (but saved as a string) from the `metadata/customer` endpoint based on `network_type` (aka social media site)
#         (in colab notebook these string lists are saved as: `facebook`,`instagram`,`tiktok` and were not created programmatically)       
#       - a json specifically filtered for each `network_type` (aka social media site) 
#         (in colab notebook, uses `post_api` function and `facebook_analytics_posts`, `instagram_analytics_posts`, and `tiktok_analytics_posts` as json data)       
#      """

#     def path(
#         self, stream_state: Mapping[str, Any] = None, 
#         stream_slice: Mapping[str, Any] = None, 
#         next_page_token: Mapping[str, Any] = None,
#         **kwargs,
#     ) -> str:
#         endpoint = f"{customer_id}/analytics/posts"
#         return endpoint

# # Source
class SourceSproutSocial(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        connection_url = f"https://api.sproutsocial.com/v1/metadata/client"
        headers = {'Authorization': f"Bearer {config['api_key']}"}
        try:
            response = requests.get(url=connection_url, headers=headers)
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        return [ClientMetadata(config=config),
                CustomerProfiles(config=config),
                CustomerTags(config=config),
                CustomerGroups(config=config),
                CustomerUsers(config=config),
                # CustomerProfileAnalytics(config=config),
                # ProfileAnalytics(config=config),
                # PostAnalytics(config=config),]
        ]
