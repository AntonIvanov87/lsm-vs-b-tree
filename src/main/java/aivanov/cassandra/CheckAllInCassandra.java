package aivanov.cassandra;

import aivanov.Edge;
import aivanov.Edges;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

class CheckAllInCassandra {

    public static void main(String[] args) throws IOException {
        var cassandraDaemon = Cassandra.start();
        try {
            var startNanos = System.nanoTime();
            var processed = 0;
            var prevProcessed = 0;
            var prevPrintNanos = System.nanoTime();

            var preparedSelect = Cassandra.prepareSelect();
            try (var lines = Files.newBufferedReader(Edges.csvFile)) {
                while (true) {
                    var line = lines.readLine();
                    if (line == null) {
                        break;
                    }
                    var edge = Edge.fromCSV(line);
                    checkEdgeInserted(edge, preparedSelect);
                    processed++;

                    var nanoTime = System.nanoTime();
                    if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
                        Edges.printProgress(prevProcessed, processed, Edges.numEdgesInCSVFile, startNanos);
                        prevProcessed = processed;
                        prevPrintNanos = nanoTime;
                    }
                }
            }

            var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
            var totalPerSec = processed / sec;
            System.out.println("Inserted " + processed + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");

        } finally {
            cassandraDaemon.deactivate();
        }
    }


    private static void checkEdgeInserted(Edge edge, ResultMessage.Prepared preparedSelect) {
        var edgeOpt = Cassandra.selectEdge(edge.sId, edge.dId, preparedSelect);
        if (edgeOpt.isEmpty()) {
            throw new IllegalStateException("Edge (" + edge.sId + ", " + edge.dId + ") has not been found after insert");
        }
        if (edgeOpt.get().st() != edge.st) {
            throw new IllegalStateException("a_st of selected edge (" + edge + ") is not equal to required st (" + edge.st + ")");
        }
    }

}
