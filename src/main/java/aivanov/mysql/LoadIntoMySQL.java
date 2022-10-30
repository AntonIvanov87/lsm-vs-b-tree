package aivanov.mysql;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import aivanov.Edge;
import aivanov.Edges;
import aivanov.SQL;

class LoadIntoMySQL {

  // TODO:
  private static final int edgesPerBatch = 100_000;
  public static final Path tempCSV = Path.of("temp.csv");

  public static void main(String[] args) throws SQLException, IOException {
    var startNanos = System.nanoTime();
    var parsed = 0L;
    var lastParsed = 0L;
    var lastPrint = System.nanoTime();

    MySQL.recreateDatabase();
    try (var conn = MySQL.createConnection()) {
      SQL.createTable(conn);

      var prevSId = 0L;
      var prevDId = 0L;
      try (var reader = Files.newBufferedReader(Edges.csvFile)) {
        var csvWriter = Files.newBufferedWriter(tempCSV, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try {
          while (true) {
            var line = reader.readLine();
            if (line == null) {
              break;
            }

            var edge = Edge.fromCSV(line);
            Edges.checkIncreasing(edge.sId, edge.dId, prevSId, prevDId);
            prevSId = edge.sId;
            prevDId = edge.dId;

            writeToCSV(edge, csvWriter);
            parsed++;

            if (parsed % edgesPerBatch == 0) {
              csvWriter.close();
              fromCSVToDB(conn);
              csvWriter = Files.newBufferedWriter(tempCSV, StandardOpenOption.TRUNCATE_EXISTING);
            }

            var nanoTime = System.nanoTime();
            if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - lastPrint) >= 10) {
              Edges.printProgress(lastParsed, parsed, Edges.numEdgesInCSVFile, startNanos);
              lastParsed = parsed;
              lastPrint = nanoTime;
            }
          }
          csvWriter.close();
          fromCSVToDB(conn);
        } finally {
          Files.delete(tempCSV);
        }
      }

      // TODO: create indices

      var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
      var totalPerSec = parsed / sec;
      System.out.println("Inserted " + parsed + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
    }
  }

  private static void writeToCSV(Edge edge, BufferedWriter csvWriter) throws IOException {
    csvWriter.write(
        String.join(
            ",",
            Long.toString(edge.sId),
            "0",
            Long.toString(edge.ut),
            Long.toString(edge.dId),
            "0",
            Long.toString(edge.ut),
            Byte.toString(edge.st),
            Long.toString(edge.ut),
            Long.toString(edge.pos),
            Byte.toString(edge.ann),
            Byte.toString(edge.st),
            Long.toString(edge.ut)
        )
    );
    csvWriter.newLine();
  }

  private static void fromCSVToDB(Connection conn) throws SQLException {
    // TODO: prepare
    try(var statement = conn.createStatement()) {
      statement.execute("LOAD DATA INFILE '" + tempCSV.toAbsolutePath() + "' INTO TABLE edges FIELDS TERMINATED BY ',' (s_id, s_st, s_ut, d_id, d_st, d_ut, st, ut, pos, ann, a_st, a_ut)");
    }
  }


}
