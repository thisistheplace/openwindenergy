#!/bin/bash

sudo timedatectl set-timezone Europe/London

# General function for checking whether services are running

function is_in_activation {
   activation=$(/sbin/service "$1" status | grep "active (running)" )
   if [ -z "$activation" ]; then
      true;
   else
      echo "Running"
      false;
   fi

   return $?;
}

function port_listening {
    if nc -z 127.0.0.1 "$1" >/dev/null ; then
        true;
    else
        false;
    fi

    return $?;
}

# Check whether installation has already been completed before

if [ -f "/usr/src/openwindenergy/INSTALLCOMPLETE" ]; then
   exit 0
fi


# Query user to set up server login credentials early on
# Ideally these values are set through Terraform apply

if [ -f "/tmp/.env" ]; then
    . /tmp/.env
fi

if [ -z "${SERVER_USERNAME}" ] || [ -z "${SERVER_PASSWORD}" ]; then
   echo "Enter username for logging into server:"
   read SERVER_USERNAME
   echo "Enter password for logging into server:"
   stty -echo
   read SERVER_PASSWORD
   stty echo
fi

# Set up large swap space

sudo fallocate -l 16G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab


# Set up general directories for Open Wind Energy application

mkdir /usr/src
mkdir /usr/src/openwindenergy

echo '' >> /usr/src/openwindenergy/log.txt
echo '========= STARTING SOFTWARE INSTALLATION =========' >> /usr/src/openwindenergy/log.txt


# Run lengthy apt-get update

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 1: Running initial apt update **********' >> /usr/src/openwindenergy/log.txt

sudo apt update -y | tee -a /usr/src/openwindenergy/log.txt

echo '********* STAGE 1: Finished running initial apt update **********' >> /usr/src/openwindenergy/log.txt


# Quickly install Apache2 so user has something to see that updates them with progress

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 2: Installing Apache2 **********' >> /usr/src/openwindenergy/log.txt

mkdir /var/www
mkdir /var/www/html
echo '<!doctype html><html><head><meta http-equiv="refresh" content="2"></head><body><pre>Beginning installation of Open Wind Energy...</pre></body></html>' | sudo tee /var/www/html/index.html
sudo apt install apache2 libapache2-mod-wsgi-py3 -y
sudo apt install certbot python3-certbot-apache -y
sudo a2enmod headers
sudo a2enmod proxy_http
sudo a2enmod rewrite
sudo a2enmod proxy_wstunnel
sudo apache2ctl restart

echo '********* STAGE 2: Finished installing Apache2 **********' >> /usr/src/openwindenergy/log.txt


# Install git

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 3: Installing git **********' >> /usr/src/openwindenergy/log.txt

echo '<!doctype html><html><head><meta http-equiv="refresh" content="2"></head><body><pre>Installing git...</pre></body></html>' | sudo tee /var/www/html/index.html
sudo apt install git -y | tee -a /usr/src/openwindenergy/log.txt

echo '********* STAGE 3: Finished installing git **********' >> /usr/src/openwindenergy/log.txt


# Install Open Wind Energy so log file in right place

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 4: Installing Open Wind Energy source code **********' >> /usr/src/openwindenergy/log.txt

echo '<!doctype html><html><head><meta http-equiv="refresh" content="2"></head><body><pre>Cloning Open Wind Energy GitHub repo and setting up admin site...</pre></body></html>' | sudo tee /var/www/html/index.html
sudo rm -R /usr/src/openwindenergy
cd /usr/src
git clone https://github.com/open-wind/openwindenergy.git
sudo apt install virtualenv pip -y | tee -a /usr/src/openwindenergy/log.txt
virtualenv -p /usr/bin/python3 /usr/src/openwindenergy/venv | tee -a /usr/src/openwindenergy/log.txt
source /usr/src/openwindenergy/venv/bin/activate
python3 -m pip install -U pip | tee -a /usr/src/openwindenergy/log.txt
python3 -m pip install -U setuptools wheel twine check-wheel-contents | tee -a /usr/src/openwindenergy/log.txt
pip install python-dotenv | tee -a /usr/src/openwindenergy/log.txt
pip install psycopg2-binary | tee -a /usr/src/openwindenergy/log.txt
pip install Jinja2 | tee -a /usr/src/openwindenergy/log.txt
pip install flask | tee -a /usr/src/openwindenergy/log.txt
pip install validators | tee -a /usr/src/openwindenergy/log.txt
cp /usr/src/openwindenergy/.env-template /usr/src/openwindenergy/.env
echo 'SERVER_BUILD=True' >> /usr/src/openwindenergy/.env
mkdir /usr/src/openwindenergy/build-cli
mkdir /usr/src/openwindenergy/build-cli/output
mkdir /usr/src/openwindenergy/build-cli/tileserver
git clone https://github.com/open-wind/openmaptiles-fonts.git
mv openmaptiles-fonts/fonts /usr/src/openwindenergy/build-cli/tileserver/fonts
echo "./openwindenergy-build-ubuntu.sh" >> /usr/src/openwindenergy/PROCESSING
sudo chown -R www-data:www-data /usr/src/openwindenergy
sudo sed -i "s/.*TILESERVER_URL.*/    TILESERVER_URL\=\/tiles/" /usr/src/openwindenergy/.env

echo "[Unit]
Description=openwindenergy-servicesmanager.service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/usr/src/openwindenergy
ExecStart=/usr/src/openwindenergy/openwindenergy-servicesmanager.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target

" | sudo tee /etc/systemd/system/openwindenergy-servicesmanager.service >/dev/null

sudo systemctl enable openwindenergy-servicesmanager.service
sudo systemctl start openwindenergy-servicesmanager.service

echo '********* STAGE 4: Finished installing Open Wind Energy source code **********' >> /usr/src/openwindenergy/log.txt


echo '********* STAGE 5: Installing nodejs, npm and frontail **********' >> /usr/src/openwindenergy/log.txt

echo '<!doctype html><html><head><meta http-equiv="refresh" content="2"></head><body><pre>Installing nodejs, npm and frontail to show install logs dynamically...</pre></body></html>' | sudo tee /var/www/html/index.html

sudo apt update -y | tee -a /usr/src/openwindenergy/log.txt
sudo apt install curl -y | tee -a /usr/src/openwindenergy/log.txt
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install netcat-traditional nodejs -y | tee -a /usr/src/openwindenergy/log.txt
sudo apt install netcat nodejs -y | tee -a /usr/src/openwindenergy/log.txt
sudo apt install nodejs -y | tee -a /usr/src/openwindenergy/log.txt
sudo apt install npm -y | tee -a /usr/src/openwindenergy/log.txt
npm i frontail -g 2>&1 | tee -a /usr/src/openwindenergy/log.txt

echo "[Unit]
Description=frontail.service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/usr/src/openwindenergy
ExecStart=frontail /usr/src/openwindenergy/log.txt --lines 32000 --ui-hide-topbar --url-path /logs
Restart=on-failure

[Install]
WantedBy=multi-user.target

" | sudo tee /etc/systemd/system/frontail.service >/dev/null


sudo systemctl enable frontail.service
sudo systemctl restart frontail.service

sudo echo "
SERVER_USERNAME=${SERVER_USERNAME}
SERVER_PASSWORD=${SERVER_PASSWORD}
" > /usr/src/openwindenergy/.env-server

sudo cp /usr/src/openwindenergy/apache/001-default-build-post.conf /etc/apache2/sites-available/.
sudo cp /usr/src/openwindenergy/apache/002-default-build-pre.conf /etc/apache2/sites-available/.

sudo a2ensite 002-default-build-pre.conf | tee -a /usr/src/openwindenergy/log.txt
sudo a2dissite 000-default.conf | tee -a /usr/src/openwindenergy/log.txt

while is_in_activation frontail ; do true; done

echo '********* frontail service running **********' >> /usr/src/openwindenergy/log.txt

while ! port_listening 9001 ; do true; done

echo '********* frontail service listening on port 9001 **********' >> /usr/src/openwindenergy/log.txt                                            

echo '<!doctype html><html><head><meta http-equiv="refresh" content="2; url=/admin" /></head><body><p>Redirecting to admin system...</p></body></html>' | sudo tee /var/www/html/index.html

sudo apache2ctl restart

echo '********* STAGE 5: Finished installing nodejs, npm and frontail **********' >> /usr/src/openwindenergy/log.txt


# Install general tools and required libraries

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 6: Installing general tools and required libraries **********' >> /usr/src/openwindenergy/log.txt

sudo NEEDRESTART_MODE=a apt install gnupg software-properties-common cmake make g++ dpkg build-essential autoconf pkg-config -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install libbz2-dev libpq-dev libboost-all-dev libgeos-dev libtiff-dev libspatialite-dev -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install libsqlite3-dev libcurl4-gnutls-dev liblua5.4-dev rapidjson-dev libshp-dev libgdal-dev gdal-bin -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install zip unzip lua5.4 shapelib ca-certificates curl nano wget pip proj-bin spatialite-bin sqlite3 -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install xvfb libglfw3-dev libuv1-dev libjpeg-turbo8 libcairo2-dev -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install libpango1.0-dev libjpeg-dev libgif-dev librsvg2-dev gir1.2-rsvg-2.0 librsvg2-2 librsvg2-common -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install libcurl4-openssl-dev libpixman-1-dev libpixman-1-0 ccache cmake ninja-build pkg-config xvfb -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install libc++-dev libc++abi-dev libpng-dev -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install libgl1-mesa-dev libgl1-mesa-dri libjpeg-dev -y | tee -a /usr/src/openwindenergy/log.txt
sudo NEEDRESTART_MODE=a apt install qgis qgis-plugin-grass -y | tee -a /usr/src/openwindenergy/log.txt

sudo apt update -y | tee -a /usr/src/openwindenergy/log.txt

echo '********* STAGE 6: Finished installing general tools and required libraries **********' >> /usr/src/openwindenergy/log.txt


# Install togeojson node library

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 7: Installing togeojson **********' >> /usr/src/openwindenergy/log.txt

npm install -g @mapbox/togeojson 2>&1 | tee -a /usr/src/openwindenergy/log.txt

echo '********* STAGE 7: Finished installing togeojson **********' >> /usr/src/openwindenergy/log.txt


# Install tileserver-gl

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 8: Installing tileserver-gl as system daemon **********' >> /usr/src/openwindenergy/log.txt

wget https://github.com/unicode-org/icu/releases/download/release-70-rc/icu4c-70rc-src.tgz | tee -a /usr/src/openwindenergy/log.txt
tar -xvf icu4c-70rc-src.tgz
sudo rm icu4c-70rc-src.tgz
cd icu/source
./configure --prefix=/opt | tee -a /usr/src/openwindenergy/log.txt
make -j | tee -a /usr/src/openwindenergy/log.txt
sudo make install | tee -a /usr/src/openwindenergy/log.txt

wget http://prdownloads.sourceforge.net/libpng/libpng-1.6.37.tar.gz | tee -a /usr/src/openwindenergy/log.txt
tar -xvf libpng-1.6.37.tar.gz
sudo rm libpng-1.6.37.tar.gz
cd libpng-1.6.37
./configure --prefix=/opt | tee -a /usr/src/openwindenergy/log.txt
make -j | tee -a /usr/src/openwindenergy/log.txt
sudo make install | tee -a /usr/src/openwindenergy/log.txt

sudo ldconfig
export LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=/opt/lib/pkgconfig:$PKG_CONFIG_PATH
sudo chmod -R 777 /usr/local
npm install -g tileserver-gl 2>&1 | tee -a /usr/src/openwindenergy/log.txt
chmod +x /usr/local/bin/tileserver-gl

echo "
[Unit]
Description=TileServer GL
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/usr/src/openwindenergy/
ExecStart=/usr/src/openwindenergy/run-tileserver-gl.sh
Restart=on-failure
Environment=PORT=8080
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target

StandardOutput=file:/var/log/tileserver-output.log
StandardError=file:/var/log/tileserver-error.log
"  | sudo tee /etc/systemd/system/tileserver.service >/dev/null

sudo /usr/bin/systemctl enable tileserver.service

echo '********* STAGE 8: Finished installing tileserver-gl as system daemon **********' >> /usr/src/openwindenergy/log.txt


# Install tilemaker

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 9: Installing tilemaker **********' >> /usr/src/openwindenergy/log.txt

cd /usr/src/openwindenergy
git clone https://github.com/systemed/tilemaker.git | tee -a /usr/src/openwindenergy/log.txt
cd tilemaker
make -j | tee -a /usr/src/openwindenergy/log.txt
sudo make install | tee -a /usr/src/openwindenergy/log.txt

echo '********* STAGE 9: Finished installing tilemaker **********' >> /usr/src/openwindenergy/log.txt


# Install tippecanoe

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 10: Installing tippecanoe **********' >> /usr/src/openwindenergy/log.txt

cd /usr/src/openwindenergy
git clone https://github.com/felt/tippecanoe.git | tee -a /usr/src/openwindenergy/log.txt
cd tippecanoe
make -j | tee -a /usr/src/openwindenergy/log.txt
sudo make install | tee -a /usr/src/openwindenergy/log.txt

echo '********* STAGE 10: Finished installing tippecanoe **********' >> /usr/src/openwindenergy/log.txt


# Install postgis

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 11: Installing PostGIS **********' >> /usr/src/openwindenergy/log.txt

sudo NEEDRESTART_MODE=a apt install postgresql-postgis -y | tee -a /usr/src/openwindenergy/log.txt

echo '********* STAGE 11: Finished installing PostGIS  **********' >> /usr/src/openwindenergy/log.txt


# Install Open Wind Energy application

echo '' >> /usr/src/openwindenergy/log.txt
echo '********* STAGE 12: Installing Open Wind Energy **********' >> /usr/src/openwindenergy/log.txt
cd /usr/src/openwindenergy
pip3 install gdal==`gdal-config --version` | tee -a /usr/src/openwindenergy/log.txt
pip3 install -r requirements.txt | tee -a /usr/src/openwindenergy/log.txt
pip3 install git+https://github.com/hotosm/osm-export-tool-python --no-deps | tee -a /usr/src/openwindenergy/log.txt
sudo service postgresql restart | tee -a /usr/src/openwindenergy/log.txt
sudo -u postgres psql -c "CREATE ROLE openwind WITH LOGIN PASSWORD 'password';" | tee -a /usr/src/openwindenergy/log.txt
sudo -u postgres createdb -O openwind openwind | tee -a /usr/src/openwindenergy/log.txt
sudo -u postgres psql -d openwind -c 'CREATE EXTENSION postgis;' | tee -a /usr/src/openwindenergy/log.txt
sudo -u postgres psql -d openwind -c 'GRANT ALL PRIVILEGES ON DATABASE openwind TO openwind;' | tee -a /usr/src/openwindenergy/log.txt

echo "[Unit]
Description=openwindenergy.service
After=network.target

[Service]
CPUWeight=1000
Type=simple
User=www-data
WorkingDirectory=/usr/src/openwindenergy
ExecStart=/usr/src/openwindenergy/build-server.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target

" | sudo tee /etc/systemd/system/openwindenergy.service >/dev/null

sudo systemctl enable openwindenergy.service

if [ -f "/tmp/.env" ]; then
    rm /tmp/.env
fi

echo 'FINISHED' >> /usr/src/openwindenergy/INSTALLCOMPLETE

echo '********* STAGE 12: Finished installing Open Wind Energy **********' >> /usr/src/openwindenergy/log.txt

echo '' >> /usr/src/openwindenergy/log.txt
echo '===================================================' >> /usr/src/openwindenergy/log.txt
echo '========= STARTUP INSTALLATION COMPLETE ===========' >> /usr/src/openwindenergy/log.txt
echo '===================================================' >> /usr/src/openwindenergy/log.txt
echo '' >> /usr/src/openwindenergy/log.txt

echo '===================================================' >> /usr/src/openwindenergy/log.txt
echo '================= STARTING BUILD ==================' >> /usr/src/openwindenergy/log.txt
echo '===================================================' >> /usr/src/openwindenergy/log.txt
echo '' >> /usr/src/openwindenergy/log.txt

echo '' >> /usr/src/openwindenergy/OPENWINDENERGY-START


