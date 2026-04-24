#!/usr/bin/env bash
set -e

set -e

echo "🚀 Starting SeaweedFS..."
docker compose -f seaweedFS/docker-compose.yaml up -d

echo "⏳ Waiting for SeaweedFS to be ready..."
sleep 5

echo "🔧 Setting environment variables..."
export SEAWEEDFS_FILERS=http://127.0.0.1:8888
export WORKER_ID=0
export LISTEN_ADDR=":9081"   # 👈 IMPORTANT

echo "▶️ Starting Go server..."
./bin/dms-seaweedfs-connector
