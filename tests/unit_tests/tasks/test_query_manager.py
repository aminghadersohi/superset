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
"""Regression coverage for the GTF downstream seam and import adapters."""

from __future__ import annotations

import importlib
import warnings
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from flask import Flask, Request

from superset.extensions.query_manager import (
    get_query_manager,
    query_manager_factory,
    QueryManagerFactory,
)
from superset.tasks.query_manager import AsyncQueryTokenError, QueryManager


class CustomManager(QueryManager):
    """An operator-owned manager with a verified token transport identity."""

    def init_app(self, app: Flask) -> None:
        """Record initialization on this app only."""
        app.config["QUERY_MANAGER_INIT_COUNT"] = (
            app.config.get("QUERY_MANAGER_INIT_COUNT", 0) + 1
        )

    def parse_channel_id_from_request(self, req: Request) -> str:
        """Stand in for a downstream verified-JWT jti reader."""
        return "verified-manager-jti"


def test_factory_app_isolation_and_idempotency() -> None:
    """Repeated downstream initialization must not replace managers or leak apps."""
    apps = [Flask("one"), Flask("two")]
    managers = []
    for app in apps:
        app.config["ASYNC_QUERY_MANAGER_CLASS"] = CustomManager
        query_manager_factory.init_app(app)
        with app.app_context():
            query_manager_factory.init_app()
            managers.append(get_query_manager())
            assert isinstance(managers[-1], CustomManager)
            assert app.config["QUERY_MANAGER_INIT_COUNT"] == 1
    assert managers[0] is not managers[1]


def test_legacy_class_config_and_new_config_precedence() -> None:
    """Warn for the alias while giving the new config precedence."""
    for config in (
        {"GLOBAL_ASYNC_QUERY_MANAGER_CLASS": CustomManager},
        {
            "GLOBAL_ASYNC_QUERY_MANAGER_CLASS": "unused.invalid.Class",
            "ASYNC_QUERY_MANAGER_CLASS": CustomManager,
        },
    ):
        app = Flask(__name__)
        app.config.update(config)
        with pytest.warns(DeprecationWarning, match="GLOBAL_ASYNC_QUERY_MANAGER_CLASS"):
            QueryManagerFactory().init_app(app)
        with app.app_context():
            assert isinstance(get_query_manager(), CustomManager)


def test_factory_import_path_and_invalid_class() -> None:
    """Load dotted paths and reject objects that do not implement the contract."""
    app = Flask(__name__)
    app.config["ASYNC_QUERY_MANAGER_CLASS"] = (
        "superset.tasks.query_manager.QueryManager"
    )
    QueryManagerFactory().init_app(app)
    with app.app_context():
        assert type(get_query_manager()) is QueryManager
    app = Flask("invalid")
    app.config["ASYNC_QUERY_MANAGER_CLASS"] = object
    with pytest.raises(TypeError, match="subclass QueryManager"):
        QueryManagerFactory().init_app(app)


def test_default_submission_and_polling_delegate_to_gtf() -> None:
    """Keep the task handshake and authorized DAO cursor semantics unchanged."""
    manager = QueryManager()
    context = Mock()
    handshake = {"task_ids": ["task-uuid"], "cursor": "2026-01-01T00:00:00"}
    with patch(
        "superset.tasks.async_queries.submit_chart_data_query_tasks",
        return_value=handshake,
    ) as submit:
        assert manager.submit(context, 7) == handshake
        submit.assert_called_once_with(context, 7)
    cursor = datetime(2026, 1, 1)
    statuses = ({"task-uuid": {"status": "success", "progress": None}}, cursor)
    with patch(
        "superset.daos.tasks.TaskDAO.get_statuses_changed_since", return_value=statuses
    ) as poll:
        assert manager.status_changes(cursor, "superset.query_object_v1") == statuses
        poll.assert_called_once_with(cursor, task_type="superset.query_object_v1")


def test_channel_hook_keeps_custom_jti_without_changing_authorization() -> None:
    """A fork can retain its verified JWT channel without a Superset cookie."""
    app = Flask(__name__)
    app.config["ASYNC_QUERY_MANAGER_CLASS"] = CustomManager
    with app.test_request_context():
        from flask import request

        assert get_query_manager().parse_channel_id_from_request(request) == (
            "verified-manager-jti"
        )


def test_default_channel_rejects_anonymous() -> None:
    """An unidentified request cannot gain a transport identity."""
    with (
        patch("superset.utils.core.get_user_id", return_value=None),
        patch(
            "superset.tasks.guest.get_current_guest_subscriber_key", return_value=None
        ),
        pytest.raises(AsyncQueryTokenError),
    ):
        QueryManager().parse_channel_id_from_request(Mock(spec=Request))


def test_deprecated_extensions_import_without_app_context() -> None:
    """Imports remain lazy; no GTF/Redis resources are started by the shims."""
    import superset.extensions as extensions

    with pytest.warns(DeprecationWarning, match="async_query_manager_factory"):
        assert extensions.async_query_manager_factory is query_manager_factory
    with pytest.warns(DeprecationWarning, match="async_query_manager"):
        proxy = extensions.async_query_manager
    app = Flask(__name__)
    with app.app_context():
        assert proxy._get_current_object() is get_query_manager()
    with pytest.raises(AttributeError):
        _ = extensions.not_an_extension


def test_deprecated_manager_import_and_submission() -> None:
    """The adapter validates access before scheduling; it returns GTF metadata."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        module = importlib.import_module("superset.async_events.async_query_manager")
    with pytest.warns(DeprecationWarning, match="deprecated"):
        importlib.reload(module)
    assert module.AsyncQueryTokenException is AsyncQueryTokenError
    manager = module.AsyncQueryManager()
    with (
        patch("superset.common.query_context_factory.QueryContextFactory") as factory,
        patch(
            "superset.commands.chart.data.get_data_command.ChartDataCommand"
        ) as command,
        patch.object(manager, "submit", return_value={"task_ids": ["uuid"]}) as submit,
    ):
        with pytest.warns(DeprecationWarning, match="submit_chart_data_job"):
            result = manager.submit_chart_data_job("jti", {}, 7)
        assert result == {"task_ids": ["uuid"]}
        command.return_value.validate.assert_called_once_with()
        submit.assert_called_once_with(factory.return_value.create.return_value, 7)


def test_deprecated_command_validates_and_uses_custom_manager() -> None:
    """The removed command remains importable, but never restores legacy jobs."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        module = importlib.import_module(
            "superset.commands.chart.data.create_async_job_command"
        )
    with pytest.warns(DeprecationWarning, match="deprecated"):
        importlib.reload(module)
    command = module.CreateAsyncChartDataJobCommand()
    with pytest.raises(RuntimeError, match="validate"):
        command.run({}, 7)
    with (
        patch.object(module, "get_query_manager") as get_manager,
        patch.object(module, "QueryContextFactory") as factory,
        patch.object(module, "ChartDataCommand") as chart_command,
    ):
        get_manager.return_value.parse_channel_id_from_request.return_value = "jti"
        command.validate(Mock(spec=Request))
        command.run({}, 7)
        chart_command.return_value.validate.assert_called_once_with()
        get_manager.return_value.submit.assert_called_once_with(
            factory.return_value.create.return_value, 7
        )
        chart_command.return_value.validate.side_effect = AsyncQueryTokenError()
        get_manager.return_value.submit.reset_mock()
        with pytest.raises(AsyncQueryTokenError):
            command.run({}, 7)
        get_manager.return_value.submit.assert_not_called()


def test_task_status_route_dispatches_to_custom_manager() -> None:
    """The protected route uses the configured reader and serializes its cursor."""
    from inspect import unwrap

    from superset.tasks.api import TaskRestApi

    api = Mock(spec=TaskRestApi)
    cursor = datetime(2026, 1, 1)
    with (
        Flask(__name__).test_request_context(
            "/?cursor=2026-01-01T00:00:00&task_type=superset.query_object_v1"
        ),
        patch("superset.extensions.query_manager.get_query_manager") as manager,
    ):
        manager.return_value.status_changes.return_value = ({}, cursor)
        unwrap(TaskRestApi.status_changes)(api)
        manager.return_value.status_changes.assert_called_once_with(
            cursor, task_type="superset.query_object_v1"
        )
        api.response.assert_called_once_with(
            200, statuses={}, cursor="2026-01-01T00:00:00"
        )


def test_task_status_route_rejects_invalid_cursor_before_dispatch() -> None:
    """Custom readers retain the REST endpoint's cursor validation."""
    from inspect import unwrap

    from superset.tasks.api import TaskRestApi

    api = Mock(spec=TaskRestApi)
    with (
        Flask(__name__).test_request_context("/?cursor=not-a-cursor"),
        patch("superset.extensions.query_manager.get_query_manager") as manager,
    ):
        unwrap(TaskRestApi.status_changes)(api)
        manager.assert_not_called()
        api.response_400.assert_called_once_with(message="Invalid cursor")
