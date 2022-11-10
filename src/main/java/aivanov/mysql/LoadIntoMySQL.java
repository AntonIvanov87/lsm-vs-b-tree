package aivanov.mysql;

import aivanov.ProgressPrinter;
import aivanov.SQL;
import aivanov.edge.BinFileIterator;
import aivanov.edge.Edge;
import aivanov.edge.Edges;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

class LoadIntoMySQL {

    private static final int edgesPerBatch = 100_000;
    public static final Path tempCSV = Path.of("temp.csv");

    public static void main(String[] args) throws SQLException, IOException {
        var progressPrinter = new ProgressPrinter(Edges.numEdgesInFile);

        MySQL.recreateDatabase();
        try (var dataSource = MySQL.createDataSource(false)) {
            try (var conn = dataSource.getConnection()) {
                SQL.createTable(conn);
                try (var edges = new BinFileIterator()) {
                    var csvWriter = Files.newBufferedWriter(tempCSV, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                    try {
                        while (edges.hasNext()) {
                            var edge = edges.next();
                            writeToCSV(edge, csvWriter);
                            progressPrinter.incProgress();

                            if (progressPrinter.processed() % edgesPerBatch == 0) {
                                csvWriter.close();
                                fromCSVToDB(conn);
                                csvWriter = Files.newBufferedWriter(tempCSV, StandardOpenOption.TRUNCATE_EXISTING);
                            }
                        }
                        csvWriter.close();
                        fromCSVToDB(conn);
                    } finally {
                        Files.delete(tempCSV);
                    }
                }

                // TODO: create indices

            }
        }

        var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - progressPrinter.startNanos);
        var totalPerSec = progressPrinter.processed() / sec;
        System.out.println("Inserted " + progressPrinter.processed() + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
    }

    private static void writeToCSV(Edge edge, BufferedWriter csvWriter) throws IOException {
        csvWriter.write(String.join(",", Long.toString(edge.sId), "0", Long.toString(edge.ut), Long.toString(edge.dId), "0", Long.toString(edge.ut), Byte.toString(edge.st), Long.toString(edge.ut), Long.toString(edge.pos), Byte.toString(edge.ann), Byte.toString(edge.st), Long.toString(edge.ut)));
        csvWriter.newLine();
    }

    private static void fromCSVToDB(Connection conn) throws SQLException {
        // TODO: prepare
        try (var statement = conn.createStatement()) {
            statement.execute("LOAD DATA INFILE '" + tempCSV.toAbsolutePath() + "' INTO TABLE edges FIELDS TERMINATED BY ',' (s_id, s_st, s_ut, d_id, d_st, d_ut, st, ut, pos, ann, a_st, a_ut)");
        }
    }


}
