#!/usr/bin/env bash
# Runs the TideFlow MTR suite against the official MariaDB server binary.
#
#   docker run --rm -v "$PWD:/work" tideflow-test bash /work/docker/run-mtr.sh [mtr options]
#
# Expects build/ha_tideflow.so (see docker/build-plugin.sh).
set -euo pipefail

MTR_DIR=/usr/share/mariadb/mariadb-test
PLUGIN_DIR=$(dirname "$(find /usr/lib -name ha_archive.so -path '*plugin*' | head -1)")

cp /work/build/ha_tideflow.so "$PLUGIN_DIR/"
ln -sfn /work/mysql-test/suite/tideflow "$MTR_DIR/suite/tideflow"

export TIDEFLOW_INSTALL_SQL=/work/storage/tideflow/sql/tideflow_install.sql

cd "$MTR_DIR"
exec ./mariadb-test-run --suite=tideflow --force --max-test-fail=0 \
  --vardir=/tmp/mtr-var "$@"
