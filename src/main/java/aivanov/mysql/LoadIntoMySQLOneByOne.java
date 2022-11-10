package aivanov.mysql;

import aivanov.ProgressPrinter;
import aivanov.SQL;
import aivanov.edge.BinFileIterator;
import aivanov.edge.Edges;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

class LoadIntoMySQLOneByOne {

    public static void main(String[] args) throws SQLException, IOException {
        var progressPrinter = new ProgressPrinter(Edges.numEdgesInFile);

        MySQL.recreateDatabase();
        try (var dataSource = MySQL.createDataSource(false)) {
            try (var conn = dataSource.getConnection()) {
                SQL.createTable(conn);

                try (var preparedInsert = conn.prepareStatement(SQL.insertQuery)) {
                    try (var edges = new BinFileIterator()) {
                        while (edges.hasNext()) {
                            var edge = edges.next();
                            SQL.insert(edge, preparedInsert);
                            progressPrinter.incProgress();
                        }

                        // TODO: create indices

                    }
                }
            }
        }

        var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - progressPrinter.startNanos);
        var totalPerSec = progressPrinter.processed() / sec;
        System.out.println("Inserted " + progressPrinter.processed() + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");

    }
}
