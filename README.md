```
docker-compose up
Run TxLogProducer application
Once finished, run BatchPlainStatementsExecutor application

# sql client; password: postgres
psql -h 127.0.0.1 -p 5555 -U postgres -W
# observe job result
select count(*) from txlog;
# committed offset
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group txlog_1m
```