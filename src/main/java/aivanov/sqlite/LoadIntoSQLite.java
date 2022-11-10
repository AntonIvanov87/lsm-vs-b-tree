package aivanov.sqlite;

import aivanov.ProgressPrinter;
import aivanov.SQL;
import aivanov.edge.BinFileIterator;
import aivanov.edge.EdgeKey;
import aivanov.edge.Edges;
import com.google.common.hash.BloomFilter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

class LoadIntoSQLite {

    private static final int edgesPerBatch = 1_000;

    public static void main(String[] args) throws SQLException, IOException {
        var progressPrinter = new ProgressPrinter(Edges.numEdgesInFile);
        try (var dataSource = SQLite.createDataSource(false)) {
            try (var conn = dataSource.getConnection()) {
                SQL.createTable(conn);
                var bloomFilter = BloomFilter.create(EdgeKey.funnel, Edges.numEdgesInFile);

                try (var preparedInsert = conn.prepareStatement(SQL.insertQuery)) {
                    try (var edges = new BinFileIterator()) {
                        conn.setAutoCommit(false);
                        while (edges.hasNext()) {
                            var edge = edges.next();
                            SQL.insert(edge, preparedInsert);
                            bloomFilter.put(new EdgeKey(edge.sId, edge.dId));
                            progressPrinter.incProgress();
                            if (progressPrinter.processed() % edgesPerBatch == 0) {
                                conn.commit();
                                conn.setAutoCommit(false);
                            }
                        }

                        conn.commit();

                        // TODO: create indices

                    }
                }

                Edges.storeBloomFilter(bloomFilter);
            }

            var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - progressPrinter.startNanos);
            var totalPerSec = progressPrinter.processed() / sec;
            System.out.println("Inserted " + progressPrinter.processed() + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
        }
    }
}
