#!/bin/bash

# Bootstrap non-interactive install script to run if not using Terraform

export SERVER_USERNAME=admin
export SERVER_PASSWORD=password

echo "SERVER_USERNAME=${SERVER_USERNAME}
SERVER_PASSWORD=${SERVER_PASSWORD}" >> /tmp/.env
sudo apt update -y
sudo apt install wget -y
wget https://raw.githubusercontent.com/open-wind/openwindenergy/refs/heads/main/openwindenergy-build-ubuntu.sh
chmod +x openwindenergy-build-ubuntu.sh
sudo ./openwindenergy-build-ubuntu.sh