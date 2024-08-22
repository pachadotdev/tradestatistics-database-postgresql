sudo -i -u postgres
pg_dump -d tradestatistics --exclude-table='public.hs_*' --exclude-table='public.sitc_*' --exclude-table='public.valuation_*' --exclude-table='public.products_correlation' -F c -b -v -f tradestatistics.sql
