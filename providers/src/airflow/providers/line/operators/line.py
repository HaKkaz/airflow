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
"""Operator for LINE."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.line.hooks.line import LineHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LineOperator(BaseOperator):
    """
    This operator allows you to post messages to LINE using LINE Messaging API.

    Takes both LINE Messaging API token directly or connection that has LINE bot token in password field.
    If both supplied, token parameter will be given precedence.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LineOperator`

    :param line_conn_id: LINE connection ID which its password is LINE messaging API token
    :param token: LINE messaging API token
    :param chat_id: LINE chat ID for a chat/group
    :param text: Message to be sent on LINE
    :param line_kwargs: Extra args to be passed to LINE PushMessageRequest object
    """

    template_fields: Sequence[str] = ("text", "chat_id")
    ui_color = "#FFBA40"

    def __init__(
        self,
        *,
        line_conn_id: str = "line_default",
        token: str | None = None,
        chat_id: str | None = None,
        text: str = "No message has been set.",
        line_kwargs: dict | None = None,
        **kwargs,
    ):
        self.chat_id = chat_id
        self.token = token
        self.line_kwargs = line_kwargs or {}
        self.text = text

        if line_conn_id is None:
            raise AirflowException("No valid LINE connection id supplied.")

        self.line_conn_id = line_conn_id

        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Call the LineHook to post the provided LINE message."""
        if self.text:
            self.line_kwargs["text"] = self.text

        line_hook = LineHook(
            line_conn_id=self.line_conn_id,
            token=self.token,
            chat_id=self.chat_id,
        )
        line_hook.send_message(self.line_kwargs)
