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

"""Bubble chart type plugin."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from numbers import Number
from typing import Any, ClassVar, Literal

from superset.mcp_service.chart.chart_utils import (
    _bubble_chart_what,
    _summarize_filters,
    map_bubble_config,
)
from superset.mcp_service.chart.plugin import BaseChartPlugin
from superset.mcp_service.chart.schemas import BubbleChartConfig, ChartError, ColumnRef
from superset.mcp_service.chart.validation.dataset_validator import (
    DatasetValidator,
    is_numeric_column,
)
from superset.mcp_service.common.error_schemas import ChartGenerationError

BubbleMetricOutputStatus = Literal["numeric", "nonnumeric", "unknown"]

_COUNT_AGGREGATES = {"COUNT", "COUNT_DISTINCT"}
_NUMERIC_AGGREGATES = {
    "SUM",
    "AVG",
    "MEDIAN",
    "PERCENTILE",
    "STDDEV",
    "STDDEV_SAMP",
    "VAR",
    "VAR_SAMP",
}
_SIMPLE_IDENTIFIER = re.compile(r'^(?:[A-Za-z_][\w$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])$')
_FUNCTION_PREFIX = re.compile(r"^([A-Za-z_][\w$]*)\s*\(")
_NUMERIC_LITERAL = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
_NONNUMERIC_SQL_TYPE = re.compile(
    r"\b(?:CHAR|VARCHAR|STRING|TEXT|BOOLEAN|BOOL|DATE|TIME|TIMESTAMP)\b",
    re.IGNORECASE,
)


def _column_output_status(name: str, dataset_context: Any) -> BubbleMetricOutputStatus:
    """Classify a physical column from authoritative dataset metadata."""
    for column in dataset_context.available_columns:
        if str(column.get("name", "")).lower() != name.lower():
            continue
        type_name = str(column.get("type") or "").strip().upper()
        if is_numeric_column(column):
            return "numeric"
        if type_name not in {"", "UNKNOWN"}:
            return "nonnumeric"
        return "unknown"
    return "unknown"


def _unquote_identifier(value: str) -> str:
    if len(value) >= 2 and (value[0], value[-1]) in {
        ('"', '"'),
        ("`", "`"),
        ("[", "]"),
    }:
        return value[1:-1]
    return value


def _matching_closing_parenthesis(  # noqa: C901
    sql: str, opening: int
) -> int | None:
    """Return the parenthesis matching ``opening``, ignoring quoted content."""
    depth = 0
    quote: str | None = None
    index = opening
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            if quote == "]":
                if char == "]":
                    quote = None
            elif char == "\\":
                # Some engines support backslash escapes in quoted values.
                index += 1
            elif char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 1
                else:
                    quote = None
        elif char in {"'", '"', "`"}:
            quote = char
        elif char == "[":
            quote = "]"
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
            if depth < 0:
                return None
        index += 1
    return None


def _strip_balanced_outer_parentheses(sql: str) -> str:
    """Remove only parentheses that enclose the complete expression."""
    while sql.startswith("("):
        closing = _matching_closing_parenthesis(sql, 0)
        if closing != len(sql) - 1:
            break
        sql = sql[1:-1].strip()
    return sql


def _parse_outer_function_call(sql: str) -> tuple[str, str] | None:
    """Parse a single balanced outermost function call, without trailing SQL."""
    match = _FUNCTION_PREFIX.match(sql)
    if match is None:
        return None
    opening = match.end() - 1
    if _matching_closing_parenthesis(sql, opening) != len(sql) - 1:
        return None
    return match.group(1).upper(), sql[opening + 1 : -1].strip()


def _sql_expression_output_status(  # noqa: C901
    expression: str | None, dataset_context: Any
) -> BubbleMetricOutputStatus:
    """Conservatively infer whether a SQL metric produces numeric values.

    This intentionally recognizes only proofs that are portable across SQL
    engines. Expressions that cannot be proven statically are validated from a
    small query result by the compile/preview paths.
    """
    if not expression or not expression.strip():
        return "unknown"
    sql = _strip_balanced_outer_parentheses(expression.strip())

    if _NUMERIC_LITERAL.fullmatch(sql):
        return "numeric"
    if sql.startswith("'") and sql.endswith("'"):
        return "nonnumeric"

    function_call = _parse_outer_function_call(sql)
    if function_call is None:
        return "unknown"
    function, argument = function_call

    if function in {"CAST", "TRY_CAST"}:
        cast_match = re.fullmatch(
            r".+\s+AS\s+(.+)", argument, re.IGNORECASE | re.DOTALL
        )
        if cast_match is None:
            return "unknown"
        cast_type = cast_match.group(1).strip()
        if is_numeric_column({"type": cast_type}):
            return "numeric"
        if _NONNUMERIC_SQL_TYPE.search(cast_type):
            return "nonnumeric"
        return "unknown"
    if function in _COUNT_AGGREGATES:
        return "numeric"
    if function in _NUMERIC_AGGREGATES:
        # These functions have numeric output when the database accepts their
        # input. Catch obvious text-column/literal mistakes before compilation.
        if _SIMPLE_IDENTIFIER.fullmatch(argument):
            return _column_output_status(_unquote_identifier(argument), dataset_context)
        if argument.startswith("'") and argument.endswith("'"):
            return "nonnumeric"
        return "numeric"
    if function in {"MIN", "MAX"}:
        if _SIMPLE_IDENTIFIER.fullmatch(argument):
            return _column_output_status(_unquote_identifier(argument), dataset_context)
        return _sql_expression_output_status(argument, dataset_context)
    return "unknown"


def bubble_metric_output_status(
    metric: ColumnRef, dataset_context: Any
) -> BubbleMetricOutputStatus:
    """Classify one typed Bubble metric's result as numeric/non-numeric/unknown."""
    if metric.aggregate in _COUNT_AGGREGATES:
        return "numeric"
    if metric.aggregate:
        if metric.name is None:
            return "unknown"
        return _column_output_status(metric.name, dataset_context)
    if metric.sql_expression:
        return _sql_expression_output_status(metric.sql_expression, dataset_context)
    if metric.saved_metric and metric.name:
        for saved_metric in dataset_context.available_metrics:
            if str(saved_metric.get("name", "")).lower() != metric.name.lower():
                continue
            status = _sql_expression_output_status(
                saved_metric.get("expression"), dataset_context
            )
            if status != "unknown":
                return status
            return "unknown"
    return "unknown"


def bubble_metrics_requiring_query_validation(
    config: Any, dataset_context: Any
) -> list[str]:
    """Return Bubble quantitative channels lacking static numeric proof."""
    if not isinstance(config, BubbleChartConfig):
        return []
    return [
        field
        for field in ("x", "y", "size")
        if bubble_metric_output_status(getattr(config, field), dataset_context)
        == "unknown"
    ]


def _is_finite_numeric(value: Any) -> bool:
    """Return whether a value is a finite real number, excluding booleans."""
    if isinstance(value, bool) or not isinstance(value, Number):
        return False
    try:
        return not isinstance(value, complex) and math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def bubble_metric_field(metric: Any) -> str | None:
    """Return the query-result field name for a native Bubble metric."""
    if isinstance(metric, str):
        return metric
    if not isinstance(metric, dict):
        return None
    label = metric.get("label")
    return label if isinstance(label, str) and label else None


def validate_bubble_query_output(
    data: list[Any],
    form_data: Mapping[str, Any],
    *,
    require_runtime_numeric_proof: bool,
) -> ChartError | None:
    """Validate Bubble query shape and quantitative output values.

    An empty result is a valid chart result when dataset metadata or portable
    expression inference already proves the three metric outputs are numeric.
    Callers that still need runtime proof must fail closed on an empty result.
    """
    entity = form_data.get("entity")
    metric_fields = {
        channel: bubble_metric_field(form_data.get(channel))
        for channel in ("x", "y", "size")
    }
    missing_config = [
        field
        for field, value in {"entity": entity, **metric_fields}.items()
        if not isinstance(value, str) or not value
    ]
    if missing_config:
        return ChartError(
            error=(
                "Bubble query requires entity, x, y, and size form fields; "
                f"missing or invalid: {', '.join(missing_config)}"
            ),
            error_type="InvalidChart",
        )
    if not data:
        if require_runtime_numeric_proof:
            return ChartError(
                error=(
                    "Bubble query returned no rows, so the numeric output of "
                    "an unproven saved or custom metric could not be verified"
                ),
                error_type="InvalidChartData",
            )
        return None
    if not all(isinstance(row, Mapping) for row in data):
        return ChartError(
            error="Bubble query returned rows in an unsupported shape",
            error_type="InvalidChartData",
        )

    result_fields = {key for row in data for key in row}
    required_result_fields = {"entity": entity, **metric_fields}
    series = form_data.get("series")
    if isinstance(series, str) and series:
        required_result_fields["series"] = series
    missing_results = [
        f"{channel} ({field})"
        for channel, field in required_result_fields.items()
        if field not in result_fields
    ]
    if missing_results:
        return ChartError(
            error=(
                "Bubble query result is missing required column(s): "
                + ", ".join(missing_results)
            ),
            error_type="InvalidChartData",
        )

    for channel, field in metric_fields.items():
        assert isinstance(field, str)
        samples = [row.get(field) for row in data if row.get(field) is not None]
        if not samples:
            return ChartError(
                error=(
                    f"Bubble {channel} result column '{field}' has no non-null "
                    "numeric values"
                ),
                error_type="InvalidChartData",
            )
        if any(not _is_finite_numeric(value) for value in samples):
            return ChartError(
                error=(
                    f"Bubble {channel} result column '{field}' must contain "
                    "finite numeric, non-boolean values"
                ),
                error_type="InvalidChartData",
            )
    return None


def _invalid_bubble_metric_output(
    field: str, metric: ColumnRef
) -> ChartGenerationError:
    label = metric.label or metric.name or metric.sql_expression or field
    return ChartGenerationError(
        error_type="invalid_bubble_metric_output",
        message=f"Bubble {field} metric '{label}' does not produce numeric values",
        details=(
            f"Bubble's {field} channel is quantitative. COUNT and COUNT_DISTINCT "
            "may aggregate any column, but SUM/AVG/MIN/MAX and other numeric "
            "Bubble metrics must produce numbers."
        ),
        suggestions=[
            "Use COUNT or COUNT_DISTINCT to count text values",
            "Choose a numeric input column for the metric",
            "Use a saved or SQL metric whose output is numeric",
        ],
        error_code="INVALID_BUBBLE_METRIC_OUTPUT",
    )


class BubbleChartPlugin(BaseChartPlugin):
    """Plugin for bubble chart type."""

    chart_type = "bubble"
    display_name = "Bubble Chart"
    native_viz_types: ClassVar[Mapping[str, str]] = {
        "bubble_v2": "Bubble Chart",
    }

    def pre_validate(
        self,
        config: dict[str, Any],
    ) -> ChartGenerationError | None:
        missing_fields = []

        if "entity" not in config:
            missing_fields.append("'entity' (category column per bubble)")
        if "x" not in config:
            missing_fields.append("'x' (metric for horizontal position)")
        if "y" not in config:
            missing_fields.append("'y' (metric for vertical position)")
        if "size" not in config:
            missing_fields.append("'size' (metric for bubble area)")

        if missing_fields:
            return ChartGenerationError(
                error_type="missing_bubble_fields",
                message=(
                    f"Bubble chart missing required fields: {', '.join(missing_fields)}"
                ),
                details=(
                    "Bubble charts plot an entity by three metrics: x and y "
                    "position each bubble and size sets its area"
                ),
                suggestions=[
                    "Add 'entity': {'name': 'country'}",
                    "Add 'x': {'name': 'gdp', 'aggregate': 'AVG'}",
                    "Add 'y': {'name': 'life_expectancy', 'aggregate': 'AVG'}",
                    "Add 'size': {'name': 'population', 'aggregate': 'SUM'}",
                ],
                error_code="MISSING_BUBBLE_FIELDS",
            )

        return None

    def extract_column_refs(self, config: Any) -> list[ColumnRef]:
        if not isinstance(config, BubbleChartConfig):
            return []
        refs: list[ColumnRef] = [config.entity, config.x, config.y, config.size]
        if config.series:
            refs.append(config.series)
        if config.order_by:
            refs.append(config.order_by)
        if config.filters:
            for f in config.filters:
                refs.append(ColumnRef(name=f.column))
        return refs

    def validate_dataset(
        self, config: Any, dataset_context: Any
    ) -> ChartGenerationError | None:
        """Require every quantitative Bubble channel to have numeric output."""
        if not isinstance(config, BubbleChartConfig):
            return None
        for field in ("x", "y", "size"):
            metric = getattr(config, field)
            if bubble_metric_output_status(metric, dataset_context) == "nonnumeric":
                return _invalid_bubble_metric_output(field, metric)
        return None

    def to_form_data(
        self, config: Any, dataset_id: int | str | None = None
    ) -> dict[str, Any]:
        return map_bubble_config(config)

    def generate_name(self, config: Any, dataset_name: str | None = None) -> str:
        what = _bubble_chart_what(config)
        context = _summarize_filters(config.filters)
        return self._with_context(what, context)

    def resolve_viz_type(self, config: Any) -> str:
        return "bubble_v2"

    def normalize_column_refs(self, config: Any, dataset_context: Any) -> Any:
        config_dict = config.model_dump()

        for key in ("entity", "series"):
            col = config_dict.get(key)
            if col and not col.get("sql_expression") and not col.get("saved_metric"):
                col["name"] = DatasetValidator.get_canonical_column_name(
                    col["name"], dataset_context
                )
        for key in ("x", "y", "size", "order_by"):
            metric = config_dict.get(key)
            if not metric:
                continue
            if metric.get("sql_expression"):
                continue
            if metric.get("saved_metric"):
                metric["name"] = DatasetValidator.get_canonical_metric_name(
                    metric["name"], dataset_context
                )
            else:
                metric["name"] = DatasetValidator.get_canonical_column_name(
                    metric["name"], dataset_context
                )
        DatasetValidator.normalize_filters(config_dict, dataset_context)
        return BubbleChartConfig.model_validate(config_dict)

    def schema_error_hint(self) -> ChartGenerationError | None:
        return ChartGenerationError(
            error_type="bubble_validation_error",
            message="Bubble chart configuration validation failed",
            details=(
                "The bubble chart configuration is missing required "
                "fields or has invalid structure"
            ),
            suggestions=[
                "Ensure 'entity' has a 'name'",
                "Ensure 'x', 'y', and 'size' each have 'name' and 'aggregate'",
                "Example: {'chart_type': 'bubble', "
                "'entity': {'name': 'country'}, "
                "'x': {'name': 'gdp', 'aggregate': 'AVG'}, "
                "'y': {'name': 'life_expectancy', 'aggregate': 'AVG'}, "
                "'size': {'name': 'population', 'aggregate': 'SUM'}}",
            ],
            error_code="BUBBLE_VALIDATION_ERROR",
        )
