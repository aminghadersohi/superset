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
# pylint: disable=unused-argument, import-outside-toplevel, protected-access

from datetime import datetime
from typing import Optional

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import make_url

from tests.unit_tests.db_engine_specs.utils import assert_convert_dttm
from tests.unit_tests.fixtures.common import dttm  # noqa: F401


def test_odbc_impersonation(mocker: MockerFixture) -> None:
    """
    Test ``impersonate_user`` method when driver == odbc.

    The method adds the parameter ``DelegationUID`` to the query string.
    """
    from sqlalchemy.engine.url import URL

    from superset.db_engine_specs.drill import DrillEngineSpec

    database = mocker.MagicMock()

    url = URL.create("drill+odbc")
    username = "DoAsUser"
    url, _ = DrillEngineSpec.impersonate_user(
        database=database,
        username=username,
        user_token=None,
        url=url,
        engine_kwargs={},
    )
    assert url.query["DelegationUID"] == username


def test_jdbc_impersonation(mocker: MockerFixture) -> None:
    """
    Test ``impersonate_user`` method when driver == jdbc.

    The method adds the parameter ``impersonation_target`` to the query string.
    """
    from sqlalchemy.engine.url import URL

    from superset.db_engine_specs.drill import DrillEngineSpec

    database = mocker.MagicMock()

    url = URL.create("drill+jdbc")
    username = "DoAsUser"
    url, _ = DrillEngineSpec.impersonate_user(
        database=database,
        username=username,
        user_token=None,
        url=url,
        engine_kwargs={},
    )
    assert url.query["impersonation_target"] == username


def test_sadrill_impersonation(mocker: MockerFixture) -> None:
    """
    Test ``impersonate_user`` method when driver == sadrill.

    The method adds the parameter ``impersonation_target`` to the query string.
    """
    from sqlalchemy.engine.url import URL

    from superset.db_engine_specs.drill import DrillEngineSpec

    database = mocker.MagicMock()

    url = URL.create("drill+sadrill")
    username = "DoAsUser"
    url, _ = DrillEngineSpec.impersonate_user(
        database=database,
        username=username,
        user_token=None,
        url=url,
        engine_kwargs={},
    )
    assert url.query["impersonation_target"] == username


def test_invalid_impersonation(mocker: MockerFixture) -> None:
    """
    Test ``impersonate_user`` method when driver == foobar.

    The method raises an exception because impersonation is not supported
    for drill+foobar.
    """
    from sqlalchemy.engine.url import URL

    from superset.db_engine_specs.drill import DrillEngineSpec
    from superset.db_engine_specs.exceptions import SupersetDBAPIProgrammingError

    database = mocker.MagicMock()

    url = URL.create("drill+foobar")
    username = "DoAsUser"

    with pytest.raises(SupersetDBAPIProgrammingError):
        DrillEngineSpec.impersonate_user(
            database=database,
            username=username,
            user_token=None,
            url=url,
            engine_kwargs={},
        )


@pytest.mark.parametrize(
    "target_type,expected_result",
    [
        ("Date", "TO_DATE('2019-01-02', 'yyyy-MM-dd')"),
        ("TimeStamp", "TO_TIMESTAMP('2019-01-02 03:04:05', 'yyyy-MM-dd HH:mm:ss')"),
        ("UnknownType", None),
    ],
)
def test_convert_dttm(
    target_type: str,
    expected_result: Optional[str],
    dttm: datetime,  # noqa: F811
) -> None:
    from superset.db_engine_specs.drill import DrillEngineSpec as spec  # noqa: N813

    assert_convert_dttm(spec, target_type, expected_result, dttm)


def test_get_schema_from_engine_params() -> None:
    """
    Test ``get_schema_from_engine_params``.
    """
    from superset.db_engine_specs.drill import DrillEngineSpec

    assert (
        DrillEngineSpec.get_schema_from_engine_params(
            make_url("drill+sadrill://localhost:8047/dfs/test?use_ssl=False"),
            {},
        )
        == "dfs.test"
    )


@pytest.mark.parametrize(
    "column_name,expected_result",
    [
        ("time", "time_07cc69"),
        ("count", "count_e2942a"),
    ],
)
def test_connect_make_label_compatible(column_name: str, expected_result: str) -> None:
    from superset.db_engine_specs.drill import (
        DrillEngineSpec as spec,  # noqa: N813
    )

    label = spec.make_label_compatible(column_name)
    assert label == expected_result
