echo "listen_addresses = '*'" >> /data/timescale/postgresql.conf
echo "host all all 0.0.0.0/0 md5" >> /data/timescale/pg_hba.conf
su postgres -s /bin/bash -c "/usr/lib/postgresql/15/bin/pg_ctl reload -D /data/timescale"
