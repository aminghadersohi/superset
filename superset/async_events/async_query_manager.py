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
"""Deprecated import adapters, not the retired Redis-stream job protocol."""

from __future__ import annotations

import warnings
from typing import Any

from superset.tasks.query_manager import AsyncQueryTokenError, QueryManager

warnings.warn(
    "superset.async_events.async_query_manager is deprecated; use "
    "superset.tasks.query_manager. Execution and polling use the GTF protocol.",
    DeprecationWarning,
    stacklevel=2,
)

AsyncQueryTokenException = AsyncQueryTokenError


class AsyncQueryManager(QueryManager):
    """Import-compatible base class delegating chart submission to GTF.

    Legacy init_job/update_job/read_events and Redis result_url replay are not
    emulated. Migrate these to GTF task lifecycle and status_changes instead.
    """

    def submit_chart_data_job(
        self,
        channel_id: str,
        form_data: dict[str, Any],
        user_id: int | None = None,
    ) -> dict[str, Any]:
        """Validate a legacy submission and return GTF task_ids/cursor, not job_id."""
        from superset.commands.chart.data.get_data_command import ChartDataCommand
        from superset.common.query_context_factory import QueryContextFactory

        warnings.warn(
            "submit_chart_data_job is deprecated; use QueryManager.submit. "
            "The return value is a GTF task_ids/cursor handshake.",
            DeprecationWarning,
            stacklevel=2,
        )
        query_context = QueryContextFactory().create(**form_data)
        ChartDataCommand(query_context).validate()
        return self.submit(query_context, user_id)
