---
title: Async-query extension migration
sidebar_position: 11
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Async-query extension migration

Global Async Queries uses Global Task Framework (GTF) execution: one task per
QueryObject, shared-task deduplication, task UUIDs and a status cursor, followed
by a synchronous re-request against the warmed query cache. The extension seam
below does not reinstate the retired Redis job/event protocol.

## Backend manager

Set `ASYNC_QUERY_MANAGER_CLASS` in `superset_config.py` to a `QueryManager`
subclass or its dotted import path. Instances belong to a Flask app, not a
process-global singleton. `init_app(app)` runs once, inside that app's context;
do not store request-specific state on the manager. Configure web and worker
apps consistently. Obtain the active instance with
`superset.extensions.query_manager.get_query_manager()`.

```python
from flask import Request
from superset.tasks.query_manager import QueryManager

class DeploymentQueryManager(QueryManager):
    def parse_channel_id_from_request(self, req: Request) -> str:
        # Use the deployment's authenticated, signature-verified JWT claims.
        # Never decode an unverified bearer token to obtain this identity.
        return verified_manager_claims()["jti"]

ASYNC_QUERY_MANAGER_CLASS = DeploymentQueryManager
```

`verified_manager_claims` above represents your authentication integration, not
an API provided by Superset. The built-in channel resolver uses the current
user/guest principal and does not require the removed async-token cookie.

Supported methods:

- `submit(query_context, user_id)`: called after the chart route's access checks
  and cache probe. Delegate to `super().submit(...)` to retain query serialization,
  effective identity/RLS, contribution DAGs, cancellation and deduplication.
  Return the GTF `task_ids`/`cursor`/optional `tab_id` handshake unchanged. Custom
  callers must authorize the QueryContext before submitting it.
- `status_changes(cursor, task_type=None)`: called by the protected Task REST
  endpoint. The default delegates to `TaskDAO.get_statuses_changed_since`, with
  subscriber/guest visibility filters. A custom reader must preserve those
  checks, the optional task-type filter, and the datetime cursor contract. This
  endpoint also serves non-chart task consumers; do not limit all reads to charts.
- `parse_channel_id_from_request(req)`: an overridable identity helper for custom
  transports and the legacy command adapter. Native GTF submission/polling does
  **not** use the returned channel to authorize tasks. A custom submission or
  polling override can call this helper in the authenticated request context.

A Manager JWT's opaque `jti` can remain the custom transport's channel ID, but it
cannot directly become the built-in websocket's routing key: GTF validates
routes against task subscribers (`user:<id>` or `guest:<hmac>`, optionally with a
per-client suffix). Use a custom authenticated transport with an authorized
mapping to GTF tasks, or migrate to GTF principal/per-tab subscriptions. A `jti`
alone must never grant access to another principal's task. No JWT secret or
cookie settings are automatically copied to the new websocket trust domain.

## Custom client polling

Frontend forks can initialize the async middleware with a typed reader:

```typescript
import { init, AsyncQueryPollingTransport } from 'src/middleware/asyncEvent';

const poll: AsyncQueryPollingTransport = async ({ cursor, task_type }) => {
  // deploymentClient authenticates using your deployment's transport.
  return deploymentClient.readTaskStatuses({ cursor, task_type });
};
init(appConfig, poll);
```

The reader receives the server cursor and task type and returns
`{ statuses: { [taskUuid]: { status, progress } }, cursor }`. Its endpoint must
read authoritative, authorized GTF state, not translate Redis stream IDs into
timestamps. It is used for both interval polling and websocket reconciliation;
backoff, waiter aggregation, failures and cache re-requests stay in Superset.
Set `WEBSOCKET_ENABLE=False` to use polling alone. Calling `init` without a
reader restores the default Task REST API transport. Install the override after
the application's normal initialization and before submitting chart requests.

## Deprecated imports and migration window

The Python import adapters and manager config alias emit `DeprecationWarning`
and remain available throughout the first major release containing this change;
removal is no earlier than the following major release. Enable Python's default
warning filter during downstream migration (`PYTHONWARNINGS=default`).

| Removed surface | Disposition |
| --- | --- |
| `superset.async_events` | Deprecated import namespace only; no API or Redis engine |
| `superset.extensions.async_query_manager_factory` | Lazy factory alias; `init_app(app)` (or current app) initializes the GTF-backed manager once |
| `superset.extensions.async_query_manager` | Lazy app-local proxy; replace with `get_query_manager()` |
| `AsyncQueryManager` | Subclassable `QueryManager` adapter; `submit_chart_data_job` validates access and delegates to GTF |
| `AsyncQueryTokenException` | Alias for `AsyncQueryTokenError`; existing exception catches retain identity |
| `create_async_job_command.CreateAsyncChartDataJobCommand` | `validate` resolves the custom channel; `run` validates QueryContext access and returns the GTF handshake |
| `init_job`, `update_job`, `read_events`, legacy cancellation and `result_url` replay | Not emulated; use GTF task lifecycle, status reads, task cancellation and chart-data re-request |

Legacy Explore task implementations belong to the downstream fork. Only the imports listed above are covered; imports of removed private task
helpers and legacy lifecycle calls still require migration to GTF. Old overrides of `submit_chart_data_job`/`read_events` are not
invoked by the native GTF chart/status routes; port them to `submit` and
`status_changes`. This layer is not a promise that an unchanged legacy subclass
can execute requests.

## Configuration disposition

| Key | Replacement or disposition |
| --- | --- |
| `GLOBAL_ASYNC_QUERY_MANAGER_CLASS` | Deprecated fallback for `ASYNC_QUERY_MANAGER_CLASS`; the new key takes precedence |
| `GLOBAL_ASYNC_QUERIES` feature flag | Retained; auto-enables GTF |
| `GLOBAL_ASYNC_QUERIES_POLLING_DELAY` | Retained, same base polling cadence |
| `GLOBAL_ASYNC_QUERIES_CACHE_BACKEND` | Removed; use `DISTRIBUTED_COORDINATION_CONFIG` for coordination and `DATA_CACHE_CONFIG` for results |
| `GLOBAL_ASYNC_QUERIES_TRANSPORT` | Removed; `WEBSOCKET_ENABLE=False` uses polling, or supply a client polling reader |
| `GLOBAL_ASYNC_QUERIES_WEBSOCKET_URL` | Removed; use `WEBSOCKET_URL` with the new websocket server |
| `GLOBAL_ASYNC_QUERIES_JWT_SECRET` | Removed; explicitly configure `WEBSOCKET_JWT_SECRET` only if using websocket transport |
| `GLOBAL_ASYNC_QUERIES_JWT_COOKIE_NAME` | Removed; `WEBSOCKET_JWT_COOKIE_NAME` (new token claims, not old-token compatibility) |
| `GLOBAL_ASYNC_QUERIES_JWT_COOKIE_SECURE` | Removed; `WEBSOCKET_JWT_COOKIE_SECURE` |
| `GLOBAL_ASYNC_QUERIES_JWT_COOKIE_SAMESITE` | Removed; `WEBSOCKET_JWT_COOKIE_SAMESITE` |
| `GLOBAL_ASYNC_QUERIES_JWT_COOKIE_DOMAIN` | Removed; `WEBSOCKET_JWT_COOKIE_DOMAIN` |
| `GLOBAL_ASYNC_QUERIES_JWT_EXPIRATION_SECONDS` | Removed; `WEBSOCKET_JWT_EXPIRATION_SECONDS` for websocket token lifetime, not task timeout |
| `GLOBAL_ASYNC_QUERIES_REGISTER_REQUEST_HANDLERS` | Removed; websocket cookie handling follows `WEBSOCKET_ENABLE`; custom manager owns its transport handlers |
| `GLOBAL_ASYNC_QUERIES_REDIS_STREAM_PREFIX` | Removed; no browser-facing GAQ Redis streams; coordination uses its own namespace |
| `GLOBAL_ASYNC_QUERIES_REDIS_STREAM_LIMIT` | Removed; no GAQ per-channel event log |
| `GLOBAL_ASYNC_QUERIES_REDIS_STREAM_LIMIT_FIREHOSE` | Removed; no GAQ firehose; use GTF task observability/retention |

The GTF-era settings `GLOBAL_ASYNC_QUERIES_DEFAULT`,
`GLOBAL_ASYNC_QUERIES_MIN_CACHE_TTL`, `GLOBAL_ASYNC_QUERIES_QUERY_TIMEOUT`,
`GLOBAL_ASYNC_QUERIES_POLLING_MAX_DELAY` and
`GLOBAL_ASYNC_QUERIES_POLLING_STALE_TIMEOUT` are unchanged. Retired settings
other than the manager alias are not interpreted or silently translated.
