sudo -i -u postgres

pg_dump -d tradestatistics --exclude-table='public.hs_*' --exclude-table='public.sitc_*' --exclude-table='public.valuation_*' --exclude-table='public.products_correlation' -F c -b -v -f tradestatistics.sql
pg_dump -d tradestatistics --table='public.hs_rev1992_*' -F c -b -v -f tradestatistics_hs_rev1992.sql
pg_dump -d tradestatistics --table='public.hs_rev2002_*' -F c -b -v -f tradestatistics_hs_rev2002.sql
pg_dump -d tradestatistics --table='public.hs_rev2007_*' -F c -b -v -f tradestatistics_hs_rev2007.sql
pg_dump -d tradestatistics --table='public.hs_rev2012_*' -F c -b -v -f tradestatistics_hs_rev2012.sql
pg_dump -d tradestatistics --table='public.sitc_rev1_*' -F c -b -v -f tradestatistics_sitc_rev1.sql
pg_dump -d tradestatistics --table='public.sitc_rev2_*' -F c -b -v -f tradestatistics_sitc_rev2.sql
pg_dump -d tradestatistics --table='public.valuation_systems_*' -F c -b -v -f tradestatistics_valuation_systems.sql

chmod 777 tradestatistics.sql
chmod 777 tradestatistics_hs_rev1992.sql
chmod 777 tradestatistics_hs_rev2002.sql
chmod 777 tradestatistics_hs_rev2007.sql
chmod 777 tradestatistics_hs_rev2012.sql
chmod 777 tradestatistics_sitc_rev1.sql
chmod 777 tradestatistics_sitc_rev2.sql
chmod 777 tradestatistics_valuation_systems.sql
