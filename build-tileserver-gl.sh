#!/bin/bash

# - Set up tileserver-gl as service
# - Change Apache2 settings to point to new tiles
# - Restart tileserver-gl and Apache2

cp /usr/src/openwindenergy/build-cli/app/index.html /usr/src/openwindenergy/admin/templates/index.html
cp /usr/src/openwindenergy/build-cli/app/*.js /usr/src/openwindenergy/admin/static/js/.

echo "<VirtualHost *:80>
ServerAdmin webmaster@localhost
ErrorLog /var/log/apache2/openwindenergy-error.log
CustomLog /var/log/apache2/openwindenergy-access.log combined

Alias /static/ /usr/src/openwindenergy/admin/static/
Alias /bounds-centre.js /usr/src/openwindenergy/admin/static/js/bounds-centre.js
Alias /datasets-latest-style.js /usr/src/openwindenergy/admin/static/js/datasets-latest-style.js

<Directory /usr/src/openwindenergy/admin/static>
    Require all granted
</Directory>

AddType application/octet-stream .gpkg .geojson .shp .dbf .prj .shx
Alias "/outputfiles/" "/usr/src/openwindenergy/build-cli/output/"
<Directory /usr/src/openwindenergy/build-cli/output>
        Options -Indexes +FollowSymLinks
        AllowOverride None
        Require all granted
</Directory>

ProxyPass /tiles/ http://localhost:8080/
ProxyPassReverse /tiles/ http://localhost:8080/
ProxyPass /logs http://localhost:9001/logs
ProxyPassReverse /logs http://localhost:9001/logs
RewriteEngine on
RewriteCond %{HTTP:Upgrade} websocket [NC]
RewriteCond %{HTTP:Connection} upgrade [NC]
RewriteRule ^/?(.*) \"ws://localhost:9001/\$1\" [P,L]
# Set up Flask admin application
WSGIScriptAlias / /usr/src/openwindenergy/admin/app.py
WSGIDaemonProcess openwindenergy-admin user=www-data group=www-data threads=5 home=/usr/src/openwindenergy/admin python-home=/usr/src/openwindenergy/venv
<directory /usr/src/openwindenergy/admin/>
    WSGIProcessGroup openwindenergy-admin
    WSGIApplicationGroup %{GLOBAL}
    WSGIScriptReloading On
    Require all granted
</directory>
</VirtualHost>
" > /etc/apache2/sites-enabled/000-default.conf

echo '********* POST-BUILD: Restarting system daemons **********' >> /usr/src/openwindenergy/log.txt

echo '' >> /usr/src/openwindenergy/RESTARTSERVICES

echo '********* POST-BUILD: Finished restarting system daemons **********' >> /usr/src/openwindenergy/log.txt

echo '' >> /usr/src/openwindenergy/log.txt
echo '======================================================================' >> /usr/src/openwindenergy/log.txt
echo '========================= TILE BUILD COMPLETE ========================' >> /usr/src/openwindenergy/log.txt
echo '================ Click "Live website" link to see results ============' >> /usr/src/openwindenergy/log.txt
echo '======================================================================' >> /usr/src/openwindenergy/log.txt
echo '' >> /usr/src/openwindenergy/log.txt


