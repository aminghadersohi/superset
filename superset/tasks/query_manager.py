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
"""App-scoped extension points for chart queries executed by GTF."""

from __future__ import annotations

from datetime import datetime
from typing import Any, TYPE_CHECKING

from flask import Flask, Request

if TYPE_CHECKING:
    from superset.common.query_context import QueryContext


class AsyncQueryTokenError(Exception):
    """The request has no usable async-query transport identity."""


class QueryManager:
    """Customize submission and polling without replacing GTF execution.

    Implementations are operator-supplied, instantiated once per Flask app, and
    must not store request identities on the instance. Status readers must retain
    GTF's subscriber/guest authorization; channel IDs are not authorization.
    """

    def init_app(self, app: Flask) -> None:
        """Initialize app-local resources (no legacy Redis streams or cookies)."""

    def submit(
        self, query_context: QueryContext, user_id: int | None
    ) -> dict[str, Any]:
        """Submit an already-authorized query context using the GTF handshake."""
        from superset.tasks.async_queries import submit_chart_data_query_tasks

        return submit_chart_data_query_tasks(query_context, user_id)

    def status_changes(
        self, cursor: datetime | None, task_type: str | None = None
    ) -> tuple[dict[str, dict[str, Any]], datetime]:
        """Read authoritative task status scoped to the current principal."""
        from superset.daos.tasks import TaskDAO

        return TaskDAO.get_statuses_changed_since(cursor, task_type=task_type)

    def parse_channel_id_from_request(self, req: Request) -> str:
        """Resolve a transport identity, independently of task authorization.

        Forks may override this to return a verified Manager-JWT jti. An opaque
        channel is usable by a custom transport, not by GTF websocket routing.
        """
        from superset.tasks.guest import get_current_guest_subscriber_key
        from superset.tasks.subscription import principal_channel
        from superset.utils.core import get_user_id

        channel = principal_channel(get_user_id(), get_current_guest_subscriber_key())
        if channel is None:
            raise AsyncQueryTokenError("No async-query principal on request")
        return channel
