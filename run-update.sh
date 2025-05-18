sudo chown -R www-data:www-data ../openwindenergy
git reset --hard origin
git pull
sudo chown -R www-data:www-data ../openwindenergy
sudo systemctl restart openwindenergy.service
sudo systemctl restart tileserver.service
sudo apache2ctl restart