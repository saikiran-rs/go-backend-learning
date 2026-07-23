CREATE KEYSPACE IF NOT EXISTS wiki
  WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS wiki.events (
  kafka_partition int,
  kafka_offset bigint,
  wiki_user text,
  server_url text,
  bot boolean,
  PRIMARY KEY (kafka_partition, kafka_offset)
);
