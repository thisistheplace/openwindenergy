#!/bin/bash

# - Set up tileserver-gl as service
# - Change Apache2 settings to point to new tiles
# - Restart tileserver-gl and Apache2

cp /usr/src/openwindenergy/build-cli/app/index.html /usr/src/openwindenergy/admin/templates/index.html
cp /usr/src/openwindenergy/build-cli/app/*.js /usr/src/openwindenergy/admin/static/js/.

echo '********* POST-BUILD: Restarting system daemons **********' >> /usr/src/openwindenergy/log.txt

# Reset build setup

echo '#!/bin/bash

./build-cli.sh

# ****************************************************************
# ***** Perform post-build setup specific to server install ******
# ****************************************************************
# Check that processing has finished (PROCESSING has been deleted) then:
# - Set up tileserver-gl as service
# - Change Apache2 settings to point to new tiles
# - Restart tileserver-gl and Apache2

if ! [ -f "PROCESSING" ]; then
    ./build-tileserver-gl.sh
fi' > /usr/src/openwindenergy/build-server.sh

echo '' >> /usr/src/openwindenergy/RESTARTSERVICES

echo '********* POST-BUILD: Finished restarting system daemons **********' >> /usr/src/openwindenergy/log.txt

echo '' >> /usr/src/openwindenergy/log.txt
echo '======================================================================' >> /usr/src/openwindenergy/log.txt
echo '========================= TILE BUILD COMPLETE ========================' >> /usr/src/openwindenergy/log.txt
echo '================ Click "Live website" link to see results ============' >> /usr/src/openwindenergy/log.txt
echo '======================================================================' >> /usr/src/openwindenergy/log.txt
echo '' >> /usr/src/openwindenergy/log.txt


