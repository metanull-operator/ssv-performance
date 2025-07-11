#!/bin/bash
set -eo pipefail

exec python ssv-performance-collector.py "$@"