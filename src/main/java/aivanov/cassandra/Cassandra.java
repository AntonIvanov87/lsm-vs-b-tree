package aivanov.cassandra;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.util.List;
import java.util.Optional;

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

    static ResultMessage.Prepared prepareSelect() {
        var prepResp = new PrepareMessage(
                "SELECT a_st, a_ut, pos, ann FROM test.edges WHERE s_id = ? and d_id = ?",
                "test"
        ).execute(
                new QueryState(ClientState.forInternalCalls("test")),
                System.nanoTime()
        );
        if (!(prepResp instanceof ResultMessage.Prepared)) {
            throw new IllegalStateException("Unexpected prepare message result: " + prepResp);
        }
        return (ResultMessage.Prepared) prepResp;
    }

    static Optional<aivanov.thriftscala.Edge> selectEdge(long sId, long dId, ResultMessage.Prepared preparedSelect) {
        var resp = new ExecuteMessage(
                preparedSelect.statementId,
                preparedSelect.resultMetadataId,
                QueryOptions.forInternalCalls(List.of(
                        LongSerializer.instance.serialize(sId),
                        LongSerializer.instance.serialize(dId)
                ))
        ).execute(
                new QueryState(ClientState.forInternalCalls("test")),
                System.nanoTime()
        );
        if (!(resp instanceof ResultMessage.Rows)) {
            throw new IllegalStateException("Unexpected SELECT edge response: " + resp);
        }
        var rows = (ResultMessage.Rows) resp;
        if (rows.result.isEmpty()) {
            return Optional.empty();
        }
        var row = rows.result.rows.get(0);
        return Optional.of(
                aivanov.thriftscala.Edge.apply(sId, dId, row.get(0).get(), row.get(1).getLong(), row.get(2).getLong(), row.get(3).get())
        );
    }

}
