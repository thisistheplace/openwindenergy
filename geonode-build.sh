#!/bin/bash


# Enforce emulation on Docker

export DOCKER_DEFAULT_PLATFORM=linux/amd64


# Clean any existing directories

sudo rm -r geonode
mkdir -p geonode
cd geonode


# Clone geonode 4.4.1

sudo rm -r geonode-project
git clone https://github.com/GeoNode/geonode-project.git
cd geonode-project
git checkout 4.4.1
cd ..


# Setup directory with settings and Django project

sudo rm -r openwindenergy-project
mkdir -p openwindenergy-project
virtualenv -p "$(which python3)" openwindenergy-project/venv
. openwindenergy-project/venv/bin/activate
pip install Django==3.2.13 
django-admin startproject --template=./geonode-project -e py,sh,md,rst,json,yml,ini,env,sample,properties -n monitoring-cron -n Dockerfile openwind openwindenergy-project


# Run docker

cd openwindenergy-project
python create-envfile.py --noinput
docker compose build
docker compose up -d
cd ../../
