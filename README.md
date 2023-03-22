# NBA Stats
<img src="https://user-images.githubusercontent.com/48928345/226491807-2e51681e-b9f4-4a06-a403-36ea5591797d.jpg" width="40%" height="40%" />

Some issues overcome that should be noted:
* When running a python module in a cron, you need to call it like `0 4 * * * cd / && /usr/local/bin/python -c 'from Nightly_Ingest import NightlyIngest; NightlyIngest().get_set_endpoint_data().close_connection()' > /proc/1/fd/1 2>/proc/1/fd/2` - Specifically you need to point to the appropriate python version `(which python)`. If you don't do this you will get a `Module Not Found` error.

* Also, when running a cron job, if there are environment variables you need, they should be provided to the cron just like: `MYSQL_HOST=mysql_svc MYSQL_PORT=3306`. Even though you may have passed them into the container, there not available in cron.

* We're building/running the nightly cron container like: `docker build -t nightly_cron .` and `docker run --name nightly_cron --network mysql__nba-net -it -d nightly_cron`
