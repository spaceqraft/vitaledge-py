#!/usr/bin/env bash
# Regenerate Python gRPC stubs from the VitalEdge proto definitions.
# Run from the vitaledge-py repository root.
set -euo pipefail

PROTO_ROOT="${HOME}/go/src/vitaledge/api/proto"
DDL_PROTO_FILE="${PROTO_ROOT}/vitaledge/v1/ddl.proto"
DML_PROTO_FILE="${PROTO_ROOT}/vitaledge/v1/dml.proto"
OUT_DIR="vitaledge/_proto/v1"

mkdir -p "${OUT_DIR}"

.venv/bin/python -m grpc_tools.protoc \
    -I "${PROTO_ROOT}" \
    --python_out="${OUT_DIR}/../.." \
    --grpc_python_out="${OUT_DIR}/../.." \
    "${DDL_PROTO_FILE}"

.venv/bin/python -m grpc_tools.protoc \
    -I "${PROTO_ROOT}" \
    --python_out="${OUT_DIR}/../.." \
    --grpc_python_out="${OUT_DIR}/../.." \
    "${DML_PROTO_FILE}"

# Move generated files out of the nested vitaledge/v1/ mirror into _proto/v1/
mv vitaledge/vitaledge/v1/ddl_pb2.py        "${OUT_DIR}/"
mv vitaledge/vitaledge/v1/ddl_pb2_grpc.py   "${OUT_DIR}/"
mv vitaledge/vitaledge/v1/dml_pb2.py        "${OUT_DIR}/"
mv vitaledge/vitaledge/v1/dml_pb2_grpc.py   "${OUT_DIR}/"
rm -rf vitaledge/vitaledge

# Patch the grpc stub import to use the internal _proto path
sed -i 's|from vitaledge\.v1 import ddl_pb2|from vitaledge._proto.v1 import ddl_pb2|g' \
    "${OUT_DIR}/ddl_pb2_grpc.py"
sed -i 's|from vitaledge\.v1 import dml_pb2|from vitaledge._proto.v1 import dml_pb2|g' \
    "${OUT_DIR}/dml_pb2_grpc.py"

echo "Stubs regenerated in ${OUT_DIR}"
