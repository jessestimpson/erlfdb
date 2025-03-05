#!/usr/bin/env bash

LOGGING_LEVEL="INFO"

set +e
for round in {1..10}
do
    echo "Round $round"
    echo "========"
    echo ""

    echo "# scripted"
    foundationdb/bindings/bindingtester/bindingtester.py erlang --test-name scripted --logging-level "$LOGGING_LEVEL" || exit 1

    echo "# api"
    foundationdb/bindings/bindingtester/bindingtester.py erlang --test-name api --num-ops 1000 --compare python --logging-level "$LOGGING_LEVEL" || exit 1

    echo "# api with concurrency 5"
    foundationdb/bindings/bindingtester/bindingtester.py erlang --test-name api --num-ops 1000 --concurrency 5 --logging-level "$LOGGING_LEVEL" || exit 1

    echo "# directory"
    foundationdb/bindings/bindingtester/bindingtester.py erlang --test-name directory --num-ops 1000 --compare --logging-level "$LOGGING_LEVEL" || exit 1

    echo "# directory hca"
    foundationdb/bindings/bindingtester/bindingtester.py erlang --test-name directory_hca --num-ops 100 --concurrency 5 --logging-level "$LOGGING_LEVEL" || exit 1
done
