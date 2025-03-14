export SQL_DSN="root:@tcp(localhost:3306)/one_api" REDIS_CONN_STRING="redis://default:@localhost:6379/0" && export SYNC_FREQUENCY=600 && ./one-api --port 3000 --log-dir ./logs
