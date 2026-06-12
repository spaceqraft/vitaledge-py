"""VitalEdge gRPC client."""

from __future__ import annotations

from collections.abc import Mapping
import grpc

from vitaledge._proto.v1 import query_pb2, query_pb2_grpc

_SDK_LANGUAGE = "python"
_SDK_VERSION = "0.1.0"
_PROTOCOL_VERSION = "1"

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 7443


class QueryResult:
    """Wraps a QueryResponse for convenient access."""

    def __init__(self, response: query_pb2.QueryResponse) -> None:
        self._response = response

    @property
    def columns(self) -> list[str]:
        return list(self._response.columns)

    @property
    def rows(self) -> list[dict]:
        return [_row_to_dict(row) for row in self._response.rows]

    @property
    def stats(self) -> dict:
        s = self._response.stats
        return {
            "rows_returned": s.rows_returned,
            "duration_ms": s.duration_ms,
            "configured_max_write_batch_bytes": _positive_int_or_none(
                s,
                "configured_max_write_batch_bytes",
            ),
            "effective_max_write_batch_bytes": _positive_int_or_none(
                s,
                "effective_max_write_batch_bytes",
            ),
            "max_write_batch_bytes_tuned": bool(
                getattr(s, "max_write_batch_bytes_tuned", False)
            ),
        }

    @property
    def max_write_batch_bytes(self) -> int | None:
        """Return the execute-time batch limit, preferring the effective tuned value."""
        stats = self._response.stats
        return (
            _positive_int_or_none(stats, "effective_max_write_batch_bytes")
            or _positive_int_or_none(stats, "configured_max_write_batch_bytes")
        )

    @property
    def warnings(self) -> list[dict]:
        return [{"code": d.code, "message": d.message} for d in self._response.warnings]

    def __repr__(self) -> str:
        return f"<QueryResult columns={self.columns} rows={len(self.rows)}>"


class ExplainResult:
    """Wraps an ExplainResponse."""

    def __init__(self, response: query_pb2.ExplainResponse) -> None:
        self._response = response

    @property
    def explain_json(self) -> bytes:
        return self._response.explain_json

    @property
    def stats(self) -> dict:
        s = self._response.stats
        return {
            "rows_returned": s.rows_returned,
            "duration_ms": s.duration_ms,
            "configured_max_write_batch_bytes": _positive_int_or_none(
                s,
                "configured_max_write_batch_bytes",
            ),
            "effective_max_write_batch_bytes": _positive_int_or_none(
                s,
                "effective_max_write_batch_bytes",
            ),
            "max_write_batch_bytes_tuned": bool(
                getattr(s, "max_write_batch_bytes_tuned", False)
            ),
        }

    @property
    def warnings(self) -> list[dict]:
        return [{"code": d.code, "message": d.message} for d in self._response.warnings]


class Capabilities:
    """Wraps a CapabilitiesResponse."""

    def __init__(self, response: query_pb2.CapabilitiesResponse) -> None:
        self._response = response

    @property
    def protocol_version(self) -> str:
        return self._response.protocol_version

    @property
    def parser_versions(self) -> list[str]:
        return list(self._response.parser_versions)

    @property
    def ir_versions(self) -> list[str]:
        return list(self._response.ir_versions)

    @property
    def prepared_query_supported(self) -> bool:
        return self._response.prepared_query_supported

    @property
    def parameter_binding(self) -> str:
        return self._response.parameter_binding

    @property
    def index_ddl_supported(self) -> bool:
        return self._response.index_ddl_supported

    @property
    def max_write_batch_bytes(self) -> int | None:
        """Return the server-advertised maximum write batch size in bytes, if known."""
        return (
            self.effective_max_write_batch_bytes
            or self.configured_max_write_batch_bytes
            or _positive_int_or_none(
                self._response,
                "max_write_batch_bytes",
                "MaxWriteBatchBytes",
                "max_write_batch_size",
                "max_batch_bytes",
            )
        )

    @property
    def configured_max_write_batch_bytes(self) -> int | None:
        return _positive_int_or_none(
            self._response,
            "configured_max_write_batch_bytes",
        )

    @property
    def effective_max_write_batch_bytes(self) -> int | None:
        return _positive_int_or_none(
            self._response,
            "effective_max_write_batch_bytes",
        )

    @property
    def max_write_batch_bytes_tuned(self) -> bool:
        return bool(getattr(self._response, "max_write_batch_bytes_tuned", False))


def _positive_int_or_none(obj: object, *field_names: str) -> int | None:
    for field_name in field_names:
        value = getattr(obj, field_name, None)
        if isinstance(value, int) and value > 0:
            return value
    return None


class VitalEdgeClient:
    """Synchronous gRPC client for VitalEdge QueryService.

    Usage::

        with VitalEdgeClient() as client:
            result = client.execute("MATCH (n) RETURN n LIMIT 10", tenant="default")
            for row in result.rows:
                print(row)
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        *,
        tenant: str = "default",
        tls: bool = False,
        tls_credentials: grpc.ChannelCredentials | None = None,
        channel_options: list[tuple] | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._tenant = tenant
        self._channel: grpc.Channel | None = None
        self._stub: query_pb2_grpc.QueryServiceStub | None = None
        self._tls = tls
        self._tls_credentials = tls_credentials
        self._channel_options = channel_options or []

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "VitalEdgeClient":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the gRPC channel."""
        target = f"{self._host}:{self._port}"
        if self._tls:
            creds = self._tls_credentials or grpc.ssl_channel_credentials()
            self._channel = grpc.secure_channel(target, creds, options=self._channel_options)
        else:
            self._channel = grpc.insecure_channel(target, options=self._channel_options)
        self._stub = query_pb2_grpc.QueryServiceStub(self._channel)

    def close(self) -> None:
        """Close the gRPC channel."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(
        self,
        cypher: str,
        *,
        parameters: Mapping[str, object] | None = None,
        tenant: str | None = None,
        read_only: bool = False,
        include_stats: bool = False,
        include_warnings: bool = False,
        timeout: float | None = None,
    ) -> QueryResult:
        """Execute a Cypher query and return a QueryResult."""
        proto_params = (
            {name: _python_to_proto_value(v) for name, v in parameters.items()}
            if parameters
            else {}
        )
        request = self._build_request(
            cypher,
            parameters=proto_params,
            tenant=tenant,
            read_only=read_only,
            include_stats=include_stats,
            include_warnings=include_warnings,
        )
        response = self._stub.Execute(request, timeout=timeout)
        return QueryResult(response)

    def explain(
        self,
        cypher: str,
        *,
        parameters: Mapping[str, object] | None = None,
        tenant: str | None = None,
        timeout: float | None = None,
    ) -> ExplainResult:
        """Request a query execution plan without running the query."""
        proto_params = (
            {name: _python_to_proto_value(v) for name, v in parameters.items()}
            if parameters
            else {}
        )
        request = self._build_request(cypher, parameters=proto_params, tenant=tenant)
        response = self._stub.Explain(request, timeout=timeout)
        return ExplainResult(response)

    def get_capabilities(self, *, timeout: float | None = None) -> Capabilities:
        """Retrieve server capabilities."""
        response = self._stub.GetCapabilities(
            query_pb2.CapabilitiesRequest(), timeout=timeout
        )
        return Capabilities(response)

    def create_property_index(
        self,
        *,
        schema: str,
        property: str,
        tenant: str | None = None,
        if_not_exists: bool = True,
        timeout: float | None = None,
    ) -> dict:
        """Create a property index for faster equality lookups and MERGE matching."""
        request = query_pb2.CreatePropertyIndexRequest(
            tenant=tenant if tenant is not None else self._tenant,
            schema=schema,
            property=property,
            if_not_exists=if_not_exists,
        )
        response = self._stub.CreatePropertyIndex(request, timeout=timeout)
        return {
            "created": response.created,
            "indexed_entities": response.indexed_entities,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_request(
        self,
        cypher: str,
        *,
        parameters: dict | None = None,
        tenant: str | None = None,
        read_only: bool = False,
        include_stats: bool = False,
        include_warnings: bool = False,
    ) -> query_pb2.QueryRequest:
        return query_pb2.QueryRequest(
            tenant=tenant if tenant is not None else self._tenant,
            input=query_pb2.QueryInput(cypher=cypher),
            options=query_pb2.RequestOptions(
                read_only=read_only,
                include_stats=include_stats,
                include_warnings=include_warnings,
            ),
            client=query_pb2.ClientContext(
                sdk_language=_SDK_LANGUAGE,
                sdk_version=_SDK_VERSION,
                protocol_version=_PROTOCOL_VERSION,
            ),
            parameters=parameters or {},
        )

    def __repr__(self) -> str:
        status = "connected" if self._channel else "disconnected"
        return f"<VitalEdgeClient {self._host}:{self._port} [{status}]>"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _python_to_proto_value(v: object) -> query_pb2.Value:
    """Convert a Python value to the proto Value type for parameter binding."""
    if v is None:
        return query_pb2.Value(null_value=query_pb2.NullValue())
    if isinstance(v, bool):
        return query_pb2.Value(bool_value=v)
    if isinstance(v, int):
        return query_pb2.Value(int_value=v)
    if isinstance(v, float):
        return query_pb2.Value(double_value=v)
    if isinstance(v, str):
        return query_pb2.Value(string_value=v)
    if isinstance(v, bytes):
        return query_pb2.Value(bytes_value=v)
    if isinstance(v, (list, tuple)):
        return query_pb2.Value(
            list_value=query_pb2.ListValue(
                values=[_python_to_proto_value(item) for item in v]
            )
        )
    if isinstance(v, Mapping):
        return query_pb2.Value(
            map_value=query_pb2.MapValue(
                values={k: _python_to_proto_value(mv) for k, mv in v.items()}
            )
        )
    raise TypeError(f"Unsupported parameter type: {type(v).__name__}")


def _value_to_python(v: query_pb2.Value):
    kind = v.WhichOneof("kind")
    if kind == "bool_value":
        return v.bool_value
    if kind == "int_value":
        return v.int_value
    if kind == "double_value":
        return v.double_value
    if kind == "string_value":
        return v.string_value
    if kind == "bytes_value":
        return v.bytes_value
    if kind == "list_value":
        return [_value_to_python(item) for item in v.list_value.values]
    if kind == "map_value":
        return {k: _value_to_python(mv) for k, mv in v.map_value.values.items()}
    return None  # null_value or unknown


def _row_to_dict(row: query_pb2.Row) -> dict:
    return {k: _value_to_python(v) for k, v in row.values.items()}



