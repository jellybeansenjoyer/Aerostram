-- Grant replication privilege to aerostream user
-- Required by Debezium's pgoutput logical decoding plugin
ALTER USER aerostream REPLICATION;
