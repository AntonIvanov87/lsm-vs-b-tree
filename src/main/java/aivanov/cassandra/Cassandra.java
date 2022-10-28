package aivanov.cassandra;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

class Cassandra {

  private Cassandra() {
  }

  static CassandraDaemon start() {
    System.setProperty("cassandra.config", "file:cassandra.yaml");
    System.setProperty("cassandra-foreground", "true");
    System.setProperty(Config.PROPERTY_PREFIX + "storagedir", "cassandra-storage");
    var cassandraDaemon = new CassandraDaemon();
    cassandraDaemon.activate();
    return cassandraDaemon;
  }

  static void createKeyspace() {
    var resp = new QueryMessage("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}", QueryOptions.DEFAULT).execute(QueryState.forInternalCalls(), System.nanoTime());
    if (!(resp instanceof ResultMessage)) {
      throw new IllegalStateException("Unexpected CREATE KEYSPACE response: " + resp);
    }
    var result = (ResultMessage) resp;
    if (result.kind != ResultMessage.Kind.SCHEMA_CHANGE && result.kind != ResultMessage.Kind.VOID) {
      throw new IllegalStateException("Unexpected CREATE KEYSPACE response kind: " + result.kind);
    }
  }

  static final String tableSchema = "CREATE TABLE IF NOT EXISTS test.edges (" +
      "s_id BIGINT, " +
      "s_st TINYINT, " +
      "s_ut BIGINT, " +
      "d_id BIGINT, " +
      "d_st TINYINT, " +
      "d_ut BIGINT, " +
      "st TINYINT, " +
      "ut BIGINT, " +
      "pos BIGINT, " +
      "ann TINYINT," +
      "a_st TINYINT, " +
      "a_ut BIGINT, " +
      "r_ut BIGINT, " +
      "PRIMARY KEY (s_id, d_id) " +
      // TODO: compaction, caching
      ")";

  static void createTable() {
    var resp = new QueryMessage(tableSchema, QueryOptions.DEFAULT).execute(
        new QueryState(ClientState.forInternalCalls("test")),
        System.nanoTime()
     );
    if (!(resp instanceof ResultMessage)) {
      throw new IllegalStateException("Unexpected CREATE TABLE response: " + resp);
    }
    var result = (ResultMessage) resp;
    if (result.kind != ResultMessage.Kind.SCHEMA_CHANGE && result.kind != ResultMessage.Kind.VOID) {
      throw new IllegalStateException("Unexpected CREATE TABLE response kind: " + result.kind);
    }
  }

  static final String insertQuery = "INSERT INTO test.edges (s_id, s_st, s_ut, d_id, d_st, d_ut, st, ut, pos, ann, a_st, a_ut) VALUES (?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?)";

}
