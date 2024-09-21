#!/bin/bash

cat << EOF
CompileFlags:
  Add:
    - -I$(asdf where erlang)/usr/include
    - -DFDB_USE_LATEST_API_VERSION=1
EOF
