#!/bin/bash
set -eo pipefail

exec python ssv-validator-count-sheets.py "$@"