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
import json
from datetime import date
from datetime import timedelta
from dateutil.relativedelta import relativedelta


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
        self.current_date = date.today()
        self.yesterday = self.current_date - timedelta(days = 1)
        self.year_ago = self.yesterday - timedelta(days = 365)

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

        return customer_id

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.config['api_key']}", 'Content-type': 'application/json'}

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
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["PageSize"] = self.page_size
        if next_page_token:
            params.update(**next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """

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
    
class TiktokProfileAnalytics(SproutSocialStream):
    primary_key = "permalink"
    http_method = "POST"
    
    """This endpoint retrieves data from the `analytics/profiles` endpoint as a post request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
      - a json specifically filtered for each `network_type` (aka social media site) 
        (in colab notebook, uses `post_api` function and `facebook_analytics_profiles`, `instagram_analytics_profiles`, and `tiktok_analytics_profiles` as json data)
      - TODO: also needs start and end dates that are input. These are hardcoded right now.     
     """
    
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        """
        Override when creating POST/PUT/PATCH requests to populate the body of the request with a JSON payload.

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        tiktok_analytics_profiles = {
            "filters": [
                "customer_profile_id.eq(5952806, 6025215)",
                f"reporting_period.in({self.year_ago}...{self.yesterday})"
            ],
            "metrics": [
                "lifetime_snapshot.followers_count",
                "lifetime_snapshot.followers_by_country",
                "lifetime_snapshot.followers_by_gender",
                "lifetime_snapshot.followers_online",
                "net_follower_growth",
                "impressions",
                "profile_views_total",
                "video_views_total"
                "comments_count_total",
                "shares_count_total",
                "likes_total",
                "posts_sent_count",
                "posts_sent_by_post_type",
            ]
            }

        return tiktok_analytics_profiles

    def path(
        self, stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/analytics/profiles"

        return endpoint

    
    
class TiktokPostAnalytics(SproutSocialStream):
    primary_key = "permalink"
    http_method = "POST"
    
    """This endpoint retrieves data from the `analytics/posts` endpoint as a post request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
      - a list (but saved as a string) from the `metadata/customer` endpoint based on `network_type` (aka social media site)
        (in colab notebook these string lists are saved as: `facebook`,`instagram`,`tiktok` and were not created programmatically)
        TODO: make this programatic. Right now `customer_profile_id` is being fed manually       
      - a json specifically filtered for each `network_type` (aka social media site) 
        (in colab notebook, uses `post_api` function and `facebook_analytics_profiles`, `instagram_analytics_profiles`, and `tiktok_analytics_profiles` as json data)       
     """
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
        ) -> Optional[Mapping[str, Any]]:
        
        tiktok_analytics_posts = {
            "fields": [
                "created_time",
                "perma_link",
                "text",
                "internal.tags.id",
                "internal.sent_by.id",
                "internal.sent_by.email",
                "internal.sent_by.first_name",
                "internal.sent_by.last_name"
            ],
            "filters": [
                "customer_profile_id.eq(5952806, 6025215)",
                f"created_time.in({self.year_ago}T00:00:00..{self.yesterday}T23:59:59)"
            ],
            "metrics": [
                "lifetime.likes",
                "lifetime.reactions",
                "lifetime.shares_count",
                "lifetime.comments_count",
                "lifetime.video_view_time_per_view",
                "lifetime.video_views_p100_per_view",
                "lifetime.impression_source_follow",
                "lifetime.impression_source_for_you",
                "lifetime.impression_source_hashtag",
                "lifetime.impression_source_personal_profile",
                "lifetime.impression_source_sound",
                "lifetime.impression_source_unspecified",
                "lifetime.video_view_time",
                "lifetime.video_views",
                "lifetime.impressions_unique",
                "lifetime.impressions",
                "lifetime.video_views",
                "video_length"
            ],
            "timezone": "America/Chicago",
            }
        return tiktok_analytics_posts
    
    def path(
        self, stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/analytics/posts"

        return endpoint
    
class FacebookProfileAnalytics(SproutSocialStream):
    primary_key = "permalink"
    http_method = "POST"
    
    """This endpoint retrieves data from the `analytics/profiles` endpoint as a post request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
      - a json specifically filtered for each `network_type` (aka social media site) 
        (in colab notebook, uses `post_api` function and `facebook_analytics_profiles`, `instagram_analytics_profiles`, and `tiktok_analytics_profiles` as json data)
      - TODO: also needs start and end dates that are input. These are hardcoded right now.     
     """
    
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        """
        Override when creating POST/PUT/PATCH requests to populate the body of the request with a JSON payload.

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        facebook_analytics_profiles = {
            "filters": [
                "customer_profile_id.eq(5931270, 5931271, 6066495)",
                f"reporting_period.in({self.year_ago}...{self.yesterday})"
            ],
            "metrics": [
                "lifetime_snapshot.followers_count",
                "lifetime_snapshot.followers_by_country",
                "lifetime_snapshot.followers_by_age_gender",
                "lifetime_snapshot.followers_by_city",
                "net_follower_growth",
                "followers_gained",
                "followers_gained_organic",
                "followers_gained_paid",
                "followers_lost",
                "impressions",
                "impressions_organic",
                "impressions_viral",
                "impressions_nonviral",
                "impressions_paid",
                "tab_views",
                "tab_views_login",
                "tab_views_logout",
                "post_impressions",
                "post_impressions_organic",
                "post_impressions_viral",
                "post_impressions_nonviral",
                "post_impressions_paid",
                "impressions_unique",
                "impressions_organic_unique",
                "impressions_viral_unique",
                "impressions_nonviral_unique",
                "impressions_paid_unique",
                "profile_views",
                "profile_views_login",
                "profile_views_logout",
                "profile_views_login_unique",
                "reactions",
                "comments_count",
                "shares_count",
                "post_link_clicks",
                "post_content_clicks_other",
                "likes",
                "reactions_love",
                "reactions_haha",
                "reactions_wow",
                "reactions_sad",
                "reactions_angry",
                "post_photo_view_clicks",
                "post_video_play_clicks",
                "profile_actions",
                "post_engagements",
                "cta_clicks_login",
                "question_answers",
                "offer_claims",
                "positive_feedback_other",
                "event_rsvps",
                "place_checkins",
                "place_checkins_mobile",
                "profile_content_activity",
                "negative_feedback",
                "video_views",
                "video_views_organic",
                "video_views_paid",
                "video_views_autoplay",
                "video_views_click_to_play",
                "video_views_repeat",
                "video_view_time",
                "video_views_unique",
                "video_views_30s_complete",
                "video_views_30s_complete_organic",
                "video_views_30s_complete_paid",
                "video_views_30s_complete_autoplay",
                "video_views_30s_complete_click_to_play",
                "video_views_30s_complete_unique",
                "video_views_30s_complete_repeat",
                "video_views_partial",
                "video_views_partial_organic",
                "video_views_partial_paid",
                "video_views_partial_autoplay",
                "video_views_partial_click_to_play",
                "video_views_partial_repeat",
                "video_views_10s",
                "video_views_10s_organic",
                "video_views_10s_paid",
                "video_views_10s_autoplay",
                "video_views_10s_click_to_play",
                "video_views_10s_repeat",
                "video_views_10s_unique",
                "posts_sent_count",
                "posts_sent_by_post_type",
                "posts_sent_by_content_type"
            ]
            }

        return facebook_analytics_profiles
    
    def error_message(self, response: requests.Response) -> str:
        return response.text

    def path(
        self, stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/analytics/profiles"

        return endpoint

    
    
class FacebookPostAnalytics(SproutSocialStream):
    primary_key = "permalink"
    http_method = "POST"
    
    """This endpoint retrieves data from the `analytics/posts` endpoint as a post request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
      - a list (but saved as a string) from the `metadata/customer` endpoint based on `network_type` (aka social media site)
        (in colab notebook these string lists are saved as: `facebook`,`instagram`,`tiktok` and were not created programmatically)
        TODO: make this programatic. Right now `customer_profile_id` is being fed manually       
      - a json specifically filtered for each `network_type` (aka social media site) 
        (in colab notebook, uses `post_api` function and `facebook_analytics_profiles`, `instagram_analytics_profiles`, and `tiktok_analytics_profiles` as json data)       
     """
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
        ) -> Optional[Mapping[str, Any]]:
        
        facebook_analytics_posts = {
            "fields": [
                "created_time",
                "perma_link",
                "text",
                "internal.tags.id",
                "internal.sent_by.id",
                "internal.sent_by.email",
                "internal.sent_by.first_name",
                "internal.sent_by.last_name"
            ],
            "filters": [
                "customer_profile_id.eq(5931270, 5931271, 6066495)",
                f"created_time.in({self.year_ago}T00:00:00..{self.yesterday}T23:59:59)"
            ],
            "metrics": [
                "lifetime.impressions",
                "lifetime.impressions_viral",
                "lifetime.impressions_nonviral",
                "lifetime.impressions_paid",
                "lifetime.impressions_follower",
                "lifetime.impressions_follower_organic",
                "lifetime.impressions_follower_paid",
                "lifetime.impressions_nonfollower",
                "lifetime.impressions_nonfollower_organic",
                "lifetime.impressions_nonfollower_paid",
                "lifetime.impressions_unique",
                "lifetime.impressions_organic_unique",
                "lifetime.impressions_viral_unique",
                "lifetime.impressions_nonviral_unique",
                "lifetime.impressions_paid_unique",
                "lifetime.impressions_follower_unique",
                "lifetime.impressions_follower_paid_unique",
                "lifetime.likes",
                "lifetime.reactions_love",
                "lifetime.reactions_haha",
                "lifetime.reactions_wow",
                "lifetime.reactions_sad",
                "lifetime.reactions_angry",
                "lifetime.shares_count",
                "lifetime.question_answers",
                "lifetime.post_content_clicks",
                "lifetime.post_photo_view_clicks",
                "lifetime.post_video_play_clicks",
                "lifetime.post_content_clicks_other",
                "lifetime.negative_feedback",
                "lifetime.engagements_unique",
                "lifetime.engagements_follower_unique",
                "lifetime.reactions_unique",
                "lifetime.comments_count_unique",
                "lifetime.shares_count_unique",
                "lifetime.question_answers_unique",
                "lifetime.post_link_clicks_unique",
                "lifetime.post_content_clicks_unique",
                "lifetime.post_photo_view_clicks_unique",
                "lifetime.post_video_play_clicks_unique",
                "lifetime.post_other_clicks_unique",
                "lifetime.negative_feedback_unique",
                "video_length",
                "lifetime.video_views",
                "lifetime.video_views_unique",
                "lifetime.video_views_organic",
                "lifetime.video_views_organic_unique",
                "lifetime.video_views_paid",
                "lifetime.video_views_paid_unique",
                "lifetime.video_views_autoplay",
                "lifetime.video_views_click_to_play",
                "lifetime.video_views_sound_on",
                "lifetime.video_views_sound_off",
                "lifetime.video_views_10s",
                "lifetime.video_views_10s_organic",
                "lifetime.video_views_10s_paid",
                "lifetime.video_views_10s_autoplay",
                "lifetime.video_views_10s_click_to_play",
                "lifetime.video_views_10s_sound_on",
                "lifetime.video_views_10s_sound_off",
                "lifetime.video_views_partial",
                "lifetime.video_views_partial_organic",
                "lifetime.video_views_partial_paid",
                "lifetime.video_views_partial_autoplay",
                "lifetime.video_views_partial_click_to_play",
                "lifetime.video_views_30s_complete",
                "lifetime.video_views_30s_complete_organic",
                "lifetime.video_views_30s_complete_paid",
                "lifetime.video_views_30s_complete_autoplay",
                "lifetime.video_views_30s_complete_click_to_play",
                "lifetime.video_views_p95",
                "lifetime.video_views_p95_organic",
                "lifetime.video_views_p95_paid",
                "lifetime.video_views_10s_unique",
                "lifetime.video_views_30s_complete_unique",
                "lifetime.video_views_p95_paid_unique",
                "lifetime.video_views_p95_organic_unique",
                "lifetime.video_view_time_per_view",
                "lifetime.video_view_time",
                "lifetime.video_view_time_organic",
                "lifetime.video_view_time_paid",
                "lifetime.video_ad_break_impressions",
                "lifetime.video_ad_break_earnings",
                "lifetime.video_ad_break_cost_per_impression",
            ],
            "timezone": "America/Chicago",
            }
        return facebook_analytics_posts



    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/analytics/posts"
        return endpoint

class InstagramProfileAnalytics(SproutSocialStream):
    primary_key = "permalink"
    http_method = "POST"
    
    """This endpoint retrieves data from the `analytics/profiles` endpoint as a post request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
      - a json specifically filtered for each `network_type` (aka social media site) 
        (in colab notebook, uses `post_api` function and `facebook_analytics_profiles`, `instagram_analytics_profiles`, and `tiktok_analytics_profiles` as json data)
      - TODO: also needs start and end dates that are input. These are hardcoded right now.     
     """
    
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        """
        Override when creating POST/PUT/PATCH requests to populate the body of the request with a JSON payload.

        At the same time only one of the 'request_body_data' and 'request_body_json' functions can be overridden.
        """
        instagram_analytics_profiles = {
            "filters": [
                "customer_profile_id.eq(5931268, 6066491)",
                f"reporting_period.in({self.year_ago}...{self.yesterday})"
            ],
            "metrics": [
                "lifetime_snapshot.followers_count",
                "lifetime_snapshot.followers_by_country",
                "lifetime_snapshot.followers_by_age_gender",
                "lifetime_snapshot.followers_by_city",
                "net_follower_growth",
                "followers_gained",
                "followers_lost",
                "lifetime_snapshot.following_count"
                "impressions",
                "impressions_unique",
                "profile_views_unique",
                "video_views"
                "reactions",
                "comments_count",
                "shares_count",
                "likes",
                "saves",
                "story_replies",
                "email_contacts",
                "get_directions_clicks",
                "phone_call_clicks",
                "text_message_clicks",
                "website_clicks",
                "posts_sent_count",
                "posts_sent_by_post_type",
                "posts_sent_by_content_type"
            ]
            }

        return instagram_analytics_profiles
    
    def error_message(self, response: requests.Response) -> str:
        return response.text

    def path(
        self, stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/analytics/profiles"

        return endpoint

    
    
class InstagramPostAnalytics(SproutSocialStream):
    primary_key = "permalink"
    http_method = "POST"
    
    """This endpoint retrieves data from the `analytics/posts` endpoint as a post request.   
    The request needs: 
      - a customer_id from ClientMetadata returned from from `{json_returned_by_ClientMetadata}['data'][0]['customer_id']`,
      - a list (but saved as a string) from the `metadata/customer` endpoint based on `network_type` (aka social media site)
        (in colab notebook these string lists are saved as: `facebook`,`instagram`,`tiktok` and were not created programmatically)
        TODO: make this programatic. Right now `customer_profile_id` is being fed manually       
      - a json specifically filtered for each `network_type` (aka social media site) 
        (in colab notebook, uses `post_api` function and `facebook_analytics_profiles`, `instagram_analytics_profiles`, and `tiktok_analytics_profiles` as json data)       
     """
    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
        ) -> Optional[Mapping[str, Any]]:
        
        instagram_analytics_posts = {
            "fields": [
                "created_time",
                "perma_link",
                "text",
                "internal.tags.id",
                "internal.sent_by.id",
                "internal.sent_by.email",
                "internal.sent_by.first_name",
                "internal.sent_by.last_name"
            ],
            "filters": [
                "customer_profile_id.eq(5931268, 6066491)",
                f"created_time.in({self.year_ago}T00:00:00..{self.yesterday}T23:59:59)"
            ],
            "metrics": [
                "lifetime.impressions",
                "lifetime.impressions_unique",
                "lifetime.likes",
                "lifetime.reactions",
                "lifetime.shares_count",
                "lifetime.comments_count",
                "lifetime.saves",
                "lifetime.story_taps_back",
                "lifetime.story_taps_forward",
                "lifetime.story_exits",
                "lifetime.video_views"
            ],
            "timezone": "America/Chicago",
            }
        return instagram_analytics_posts



    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:
        
        customer_id = self._get_customer_id()
        endpoint = f"{customer_id}/analytics/posts"
        return endpoint


# # Source
class SourceSproutSocial(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        connection_url = f"https://api.sproutsocial.com/v1/metadata/client"
        headers = {'Authorization': f"Bearer {config['api_key']}", 'Content-type': 'application/json'}
        try:
            response = requests.get(url=connection_url, headers=headers)
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        return [ClientMetadata(config=config),
                CustomerProfiles(config=config),
                CustomerTags(config=config),
                CustomerGroups(config=config),
                CustomerUsers(config=config),
                TiktokProfileAnalytics(config=config),
                TiktokPostAnalytics(config=config),
                FacebookProfileAnalytics(config=config),
                FacebookPostAnalytics(config=config),
                InstagramProfileAnalytics(config=config),
                InstagramPostAnalytics(config=config),]
        
        
