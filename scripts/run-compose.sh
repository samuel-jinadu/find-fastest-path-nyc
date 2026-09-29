#!/bin/bash

docker compose up --no-color --timestamps 2>&1 | tee compose.log