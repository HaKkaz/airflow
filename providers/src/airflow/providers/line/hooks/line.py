#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Hook for LINE."""

from __future__ import annotations

from typing import Any

from linebot.v3.messaging import ApiClient, Configuration, MessagingApi, PushMessageRequest, TextMessage

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class LineHook(BaseHook):
    """
    Creates a LINE API Connection to be used for calls.

    .. seealso::
        - :ref:`Slack API connection <howto/connection:slack>`
        - https://developers.line.biz/en/docs/messaging-api/
        - https://github.com/line/line-bot-sdk-python

    Examples:
    .. code-block:: python
        # Create hook
        line_hook = LineHook(line_conn_id="line_default")
        line_hook = LineHook()  # will use line_default
        # or line_hook = LineHook(line_conn_id='line_default', chat_id='xxxx')
        # or line_hook = LineHook(token='channel_access_token', chat_id='xxxx')

        # Call method from line bot client
        line_hook.send_message(None, {"text": "message", "chat_id": "xxxx"})
        # or line_hook.send_message(None', {"text": "message"})

    :param line_conn_id: connection that optionally has Line API token in the password field
    :param token: optional line API token
    :param chat_id: optional chat_id of the line chat/channel/group
    """

    conn_name_attr = "line_conn_id"
    default_conn_name = "line_api_default"
    conn_type = "line"
    hook_name = "LINE API"

    def __init__(
        self,
        line_conn_id: str | None = default_conn_name,
        token: str | None = None,
        chat_id: str | None = None,
    ) -> None:
        super().__init__()
        self.token = self.__get_token(token, line_conn_id)
        self.chat_id = self.__get_chat_id(chat_id, line_conn_id)
        self.connection = self.get_conn()

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "extra", "login", "port", "extra"],
            "relabeling": {},
        }

    def get_conn(self) -> MessagingApi:
        """
        Return the line bot api interface.

        :return: line bot api interface
        """
        configuration = Configuration(access_token=self.token)
        return MessagingApi(ApiClient(configuration))

    def __get_token(self, token: str | None, line_conn_id: str | None) -> str:
        """
        Return the line bot messaging API token.

        :param token: line bot messaging API  token
        :param line_conn_id: line connection name
        :return: line API token
        """
        if token is not None:
            return token

        if line_conn_id is not None:
            conn = self.get_connection(line_conn_id)

            if not conn.password:
                raise AirflowException("Missing token(password) in line connection")

            return conn.password

        raise AirflowException("Cannot get token: No valid line connection supplied.")

    def __get_chat_id(self, chat_id: str | None, line_conn_id: str | None) -> str | None:
        """
        Return the line chat ID for a chat/group.

        :param chat_id: optional chat ID
        :param line_conn_id: line connection name
        :return: line chat ID
        """
        if chat_id is not None:
            return chat_id

        if line_conn_id is not None:
            conn = self.get_connection(line_conn_id)
            return conn.host

        return None

    def send_message(self, api_params: dict) -> None:
        """
        Send the message to a line chat or group.

        :param api_params: params for PushMessageRequest. It can also be used to override chat_id.
        """
        kwargs: dict[str, str | list[TextMessage]] = {}

        if self.chat_id is not None:
            kwargs["chat_id"] = self.chat_id
        kwargs.update(api_params)

        if "messages" not in kwargs or kwargs["messages"] is None:
            raise AirflowException("'messages' must be provided for line message")

        if kwargs.get("chat_id") is None:
            raise AirflowException("'chat_id' must be provided for line message")

        kwargs["to"] = kwargs.pop("chat_id")
        kwargs["messages"] = [TextMessage(text=message) for message in kwargs["messages"]]

        response = self.connection.push_message(
            PushMessageRequest(**kwargs),
            async_req=True,
        )
        self.log.debug(response)
