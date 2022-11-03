package aivanov.cassandra;

import aivanov.Edges;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

class CheckRandomInCassandra {

    private static final int attempts = 10_000_000;

    public static void main(String[] args) {
        var cassandraDaemon = Cassandra.start();
        try {
            var startNanos = System.nanoTime();
            var processed = 0;
            var prevProcessed = 0;
            var prevPrintNanos = System.nanoTime();

            var exists = 0L;
            var preparedSelect = Cassandra.prepareSelect();
            var rand = ThreadLocalRandom.current();
            while (processed < attempts) {
                var sId = rand.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
                var dId = rand.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
                if (Cassandra.selectEdge(sId, dId, preparedSelect).isPresent()) {
                    exists++;
                }
                processed++;

                var nanoTime = System.nanoTime();
                if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
                    Edges.printProgress(prevProcessed, processed, attempts, startNanos);
                    System.out.println("Hit rate = " + (100.0 * exists / processed) + "%");
                    prevProcessed = processed;
                    prevPrintNanos = nanoTime;
                }
            }

            var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
            var totalPerSec = processed / sec;
            System.out.println("Checked " + processed + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");

        } finally {
            cassandraDaemon.deactivate();
        }
    }

}
