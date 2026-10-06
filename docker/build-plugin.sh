#!/usr/bin/env bash
# Builds ha_tideflow.so inside the tideflow-dev image.
#
#   docker run --rm -v "$PWD:/work" -v tideflow-build:/build \
#     -v tideflow-cargo:/usr/local/cargo/registry tideflow-dev docker/build-plugin.sh
#
# The MariaDB build directory lives in the tideflow-build volume, so only the
# first run pays for configuring the server tree.
set -euo pipefail

: "${MARIADB_SRC:=/src/mariadb}"
: "${MARIADB_BUILD:=/build/mariadb}"
OUT_DIR=/work/build

# Make the engine part of the server source tree.
ln -sfn /work/storage/tideflow "$MARIADB_SRC/storage/tideflow"

if [ ! -f "$MARIADB_BUILD/build.ninja" ]; then
  # Mirror the official release configuration so the plugin ABI matches the
  # server binary from the mariadb:11.4 image.
  cmake -S "$MARIADB_SRC" -B "$MARIADB_BUILD" -G Ninja \
    -DBUILD_CONFIG=mysql_release \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO \
    -DPLUGIN_CONNECT=NO -DPLUGIN_COLUMNSTORE=NO -DPLUGIN_S3=NO \
    -DPLUGIN_OQGRAPH=NO -DPLUGIN_TOKUDB=NO \
    -DWITH_UNIT_TESTS=OFF -DWITH_EMBEDDED_SERVER=OFF
fi

cmake --build "$MARIADB_BUILD" --target tideflow

mkdir -p "$OUT_DIR"
cp "$MARIADB_BUILD/storage/tideflow/ha_tideflow.so" "$OUT_DIR/"
echo "Built $OUT_DIR/ha_tideflow.so"
