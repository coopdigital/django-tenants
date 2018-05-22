#!/bin/bash

set -e

DATABASE=${DATABASE_HOST:-localhost}
echo "Database: $DATABASE"

while ! nc "$DATABASE" "5432" >/dev/null 2>&1 < /dev/null; do
  i=`expr $i + 1`
    if [ $i -ge 50 ]; then
        echo "$(date) - $DATABASE:5432 still not reachable, giving up"
        exit 1
    fi
    echo "$(date) - waiting for $DATABASE:5432..."
    sleep 1
done
echo "postgres connection established"

pushd dts_test_project

EXECUTORS=( standard multiprocessing )

for executor in "${EXECUTORS[@]}"; do
    if [ "$KEEPDB" = true ] ; then
        EXECUTOR=$executor python3 manage.py test -k django_tenants.tests
    else
        EXECUTOR=$executor python3 manage.py test django_tenants.tests
    fi
done
