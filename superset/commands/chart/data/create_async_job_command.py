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
"""Deprecated chart submission adapter for downstream imports."""

from __future__ import annotations

import warnings
from typing import Any

from flask import Request

from superset.commands.chart.data.get_data_command import ChartDataCommand
from superset.common.query_context_factory import QueryContextFactory
from superset.extensions.query_manager import get_query_manager

warnings.warn(
    "CreateAsyncChartDataJobCommand is deprecated; use QueryManager.submit "
    "with an authorized QueryContext. Results use the GTF task_ids/cursor protocol.",
    DeprecationWarning,
    stacklevel=2,
)


class CreateAsyncChartDataJobCommand:
    """Preserve validate/run imports while submitting through GTF."""

    _async_channel_id: str | None = None

    def validate(self, request: Request) -> None:
        """Resolve the downstream transport identity from an authenticated request."""
        self._async_channel_id = get_query_manager().parse_channel_id_from_request(
            request
        )

    def run(self, form_data: dict[str, Any], user_id: int | None) -> dict[str, Any]:
        """Authorize chart access before submitting and returning a GTF handshake."""
        if not self._async_channel_id:
            raise RuntimeError("Call validate() before run()")
        query_context = QueryContextFactory().create(**form_data)
        ChartDataCommand(query_context).validate()
        return get_query_manager().submit(query_context, user_id)
