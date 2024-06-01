#!/bin/bash
bash ./cleanup_environment.sh \
&& mkdir "temp" \
&& mkdir "logs" \
&& docker compose --file="./docker-compose.yml" --env-file="./.env_development" up --build

