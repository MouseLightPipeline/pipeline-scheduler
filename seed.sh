#!/usr/bin/env bash

echo "Seed postgres service"
sequelize db:seed:all
