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
"""Flask lifecycle for the GTF async-query extension seam."""

from __future__ import annotations

import warnings
from typing import cast

from flask import current_app, Flask
from werkzeug.utils import import_string

from superset.tasks.query_manager import QueryManager

_EXTENSION_KEY = "superset.query_manager"


class QueryManagerFactory:
    """Keep managers app-local, including in multi-app worker processes."""

    def init_app(self, app: Flask | None = None) -> None:
        """Initialize once; legacy callers may use the current app context."""
        if app is None:
            app = current_app._get_current_object()  # noqa: SLF001
        if _EXTENSION_KEY in app.extensions:
            return
        manager_class = app.config.get("ASYNC_QUERY_MANAGER_CLASS")
        legacy_class = app.config.get("GLOBAL_ASYNC_QUERY_MANAGER_CLASS")
        if legacy_class:
            warnings.warn(
                "GLOBAL_ASYNC_QUERY_MANAGER_CLASS is deprecated; use "
                "ASYNC_QUERY_MANAGER_CLASS and the GTF QueryManager contract.",
                DeprecationWarning,
                stacklevel=2,
            )
        manager_class = manager_class or legacy_class or QueryManager
        if isinstance(manager_class, str):
            manager_class = import_string(manager_class)
        if not isinstance(manager_class, type) or not issubclass(
            manager_class, QueryManager
        ):
            raise TypeError("ASYNC_QUERY_MANAGER_CLASS must subclass QueryManager")
        manager = manager_class()
        with app.app_context():
            manager.init_app(app)
        app.extensions[_EXTENSION_KEY] = manager

    @property
    def instance(self) -> QueryManager:
        """Return the current app's manager, initializing on first use if needed."""
        self.init_app()
        return cast(QueryManager, current_app.extensions[_EXTENSION_KEY])


query_manager_factory = QueryManagerFactory()


def get_query_manager() -> QueryManager:
    """Return the manager for the active Flask app."""
    return query_manager_factory.instance
