#!/bin/bash

set -e

echo "======================================"
echo " Jamin Industrial Agent"
echo " Version: v1.0.0-beta2"
echo "======================================"

wait_for_service() {
    local host=$1
    local port=$2
    local service=$3
    local timeout=${4:-60}

    echo "Waiting for $service ($host:$port)..."
    for i in $(seq 1 $timeout); do
        if nc -z "$host" "$port" 2>/dev/null; then
            echo "$service is ready"
            return 0
        fi
        sleep 1
    done

    echo "$service connection timed out"
    return 1
}

case "${1:-production}" in
    production)
        echo "Starting production runtime..."
        wait_for_service postgres 5432 "Postgres"
        wait_for_service influxdb 8086 "InfluxDB"
        wait_for_service redis 6379 "Redis"
        mkdir -p /app/logs
        exec gunicorn \
            -w ${WORKERS:-4} \
            -k uvicorn.workers.UvicornWorker \
            --bind 0.0.0.0:8600 \
            --access-logfile /app/logs/access.log \
            --error-logfile /app/logs/error.log \
            --log-level ${LOG_LEVEL:-info} \
            --timeout 60 \
            --keep-alive 5 \
            --max-requests 1000 \
            --max-requests-jitter 100 \
            src.api.main:app
        ;;

    development)
        echo "Starting development runtime..."
        wait_for_service postgres 5432 "Postgres" 10 || true
        wait_for_service influxdb 8086 "InfluxDB" 10 || true
        exec python -m uvicorn \
            src.api.main:app \
            --host 0.0.0.0 \
            --port 8600 \
            --reload \
            --log-level debug
        ;;

    migrate)
        echo "Preparing runtime database..."
        python scripts/manage_database.py init
        echo "Runtime database initialization completed"
        ;;

    seed-demo)
        echo "Seeding runtime database with demo data..."
        python scripts/manage_database.py seed --include-demo-data
        echo "Demo data seeding completed"
        ;;

    test)
        echo "Running tests..."
        exec pytest tests/ -v --tb=short
        ;;

    shell)
        echo "Opening shell..."
        exec /bin/bash
        ;;

    *)
        echo "Unknown command: $1"
        echo "Available commands: production, development, migrate, seed-demo, test, shell"
        exit 1
        ;;
esac
