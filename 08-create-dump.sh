# Create SQL dump and restore

sudo -i -u postgres
pg_dump tradestatistics > tradestatistics.dump
exit
sudo mv /var/lib/postgres/tradestatistics.dump ~/Downloads/tradestatistics.dump
sudo chmod 777 ~/Downloads/tradestatistics.dump
rsync -av --update ~/Downloads/tradestatistics.dump  pacha@195.238.123.198:/mnt/storage/tradestatistics.dump
ssh pacha@195.238.123.198
sudo -i -u postgres
psql

drop database tradestatistics;
create database tradestatistics;
create user USER with password 'PASSWORD';
\q

psql -d tradestatistics < /tradestatistics/tradestatistics.dump
