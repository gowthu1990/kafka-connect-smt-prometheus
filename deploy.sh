#!/bin/bash
echo "==> Cleaning and Building Fat JAR..."
./gradlew clean build shadowJar

echo "==> Copying JAR to plugins directory..."
cp ./build/libs/pii-masking-smt-*-fat.jar ./plugins/

echo "==> Restarting Kafka Connect container..."
docker compose restart local-connect

echo "==> Waiting for Kafka Connect API to become available..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8083/)" != "200" ]]; do
  sleep 5
  echo "Waiting..."
done

echo "==> Deployment Complete!"