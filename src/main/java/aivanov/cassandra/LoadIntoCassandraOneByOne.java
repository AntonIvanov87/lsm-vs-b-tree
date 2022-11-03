package aivanov.cassandra;

import aivanov.Edge;
import aivanov.Edges;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.serializers.ByteSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

class LoadIntoCassandraOneByOne {

    public static void main(String[] args) throws IOException {
        var cassandraDaemon = Cassandra.start();
        try {
            var startNanos = System.nanoTime();
            var parsed = 0;
            var prevParsed = 0;
            var prevPrintNanos = System.nanoTime();

            Cassandra.createKeyspace();
            Cassandra.createTable();

            var preparedInsert = prepareInsert();
            var prevSId = -1L;
            var prevDId = -1L;
            try (var lines = Files.newBufferedReader(Edges.csvFile)) {
                while (true) {
                    var line = lines.readLine();
                    if (line == null) {
                        break;
                    }
                    var edge = Edge.fromCSV(line);

                    Edges.checkIncreasing(edge.sId, edge.dId, prevSId, prevDId);
                    prevSId = edge.sId;
                    prevDId = edge.dId;

                    insert(edge, preparedInsert);
                    parsed++;

                    var nanoTime = System.nanoTime();
                    if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
                        Edges.printProgress(prevParsed, parsed, Edges.numEdgesInCSVFile, startNanos);
                        prevParsed = parsed;
                        prevPrintNanos = nanoTime;
                    }
                }
            }

            // TODO: create indices

            var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
            var totalPerSec = parsed / sec;
            System.out.println("Inserted " + parsed + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
        } finally {
            cassandraDaemon.deactivate();
        }
    }

    private static ResultMessage.Prepared prepareInsert() {
        var prepResp = new PrepareMessage(Cassandra.insertQuery, "test").execute(new QueryState(ClientState.forInternalCalls("test")), System.nanoTime());
        if (!(prepResp instanceof ResultMessage.Prepared)) {
            throw new IllegalStateException("Unexpected prepare message result: " + prepResp);
        }
        return (ResultMessage.Prepared) prepResp;
    }

    private static void insert(Edge edge, ResultMessage.Prepared preparedInsert) {
        var resp = new ExecuteMessage(preparedInsert.statementId, preparedInsert.resultMetadataId, QueryOptions.forInternalCalls(List.of(LongSerializer.instance.serialize(edge.sId), ByteSerializer.instance.serialize((byte) 0), LongSerializer.instance.serialize(edge.ut),

                LongSerializer.instance.serialize(edge.dId), ByteSerializer.instance.serialize((byte) 0), LongSerializer.instance.serialize(edge.ut),

                ByteSerializer.instance.serialize(edge.st), LongSerializer.instance.serialize(edge.ut), LongSerializer.instance.serialize(edge.pos), ByteSerializer.instance.serialize(edge.ann),

                ByteSerializer.instance.serialize(edge.st), LongSerializer.instance.serialize(edge.ut)))).execute(new QueryState(ClientState.forInternalCalls("test")), System.nanoTime());
        if (!(resp instanceof ResultMessage.Void)) {
            throw new IllegalStateException("Unexpected SELECT edge response: " + resp);
        }
    }

}
