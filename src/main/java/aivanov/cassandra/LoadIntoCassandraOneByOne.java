package aivanov.cassandra;

import aivanov.ProgressPrinter;
import aivanov.edge.BinFileIterator;
import aivanov.edge.Edge;
import aivanov.edge.Edges;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.serializers.ByteSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

class LoadIntoCassandraOneByOne {

    public static void main(String[] args) throws IOException {
        var cassandraDaemon = Cassandra.start();
        try {
            var progressPrinter = new ProgressPrinter(Edges.numEdgesInFile);

            Cassandra.createKeyspace();
            Cassandra.createTable();

            var preparedInsert = prepareInsert();
            try (var edges = new BinFileIterator()) {
                while (edges.hasNext()) {
                    var edge = edges.next();
                    insert(edge, preparedInsert);
                    progressPrinter.incProgress();
                }
            }

            // TODO: create indices

            var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - progressPrinter.startNanos);
            var totalPerSec = progressPrinter.processed() / sec;
            System.out.println("Inserted " + progressPrinter.processed() + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
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
