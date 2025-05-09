#!/bin/bash

# Script to restart services once build has completed

while true
    do
        sleep 1
        if [ -f "/usr/src/openwindenergy/RESTARTSERVICES" ]; then
            echo "Restarting tileserver.service and apache2 with post-build conf"
            sudo /usr/bin/systemctl restart tileserver.service
            sudo a2ensite 001-default-build-post.conf
            sudo a2dissite 002-default-build-pre.conf
            sudo /usr/sbin/apache2ctl restart
            rm /usr/src/openwindenergy/RESTARTSERVICES
        fi

        if [ -f "/usr/src/openwindenergy/OPENWINDENERGY-START" ]; then
            echo "Starting openwindenergy.service"
            sudo /usr/bin/systemctl restart openwindenergy.service
            rm /usr/src/openwindenergy/OPENWINDENERGY-START
        fi

        if [ -f "/usr/src/openwindenergy/OPENWINDENERGY-STOP" ]; then
            echo "Stopping openwindenergy.service"
            sudo /usr/bin/systemctl stop openwindenergy.service
            rm /usr/src/openwindenergy/OPENWINDENERGY-STOP
        fi

    done