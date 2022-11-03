package aivanov.mysql;

import aivanov.Edge;
import aivanov.Edges;
import aivanov.SQL;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

class LoadIntoMySQLOneByOne {

  public static void main(String[] args) throws SQLException, IOException {
    var startNanos = System.nanoTime();
    var inserted = 0L;
    var lastInserted = 0L;
    var lastPrint = System.nanoTime();

    MySQL.recreateDatabase();
    try (var conn = MySQL.createConnection()) {
      SQL.createTable(conn);

      try (var preparedInsert = conn.prepareStatement(SQL.insertQuery)) {
        var prevSId = 0L;
        var prevDId = 0L;
        try (var reader = Files.newBufferedReader(Edges.csvFile)) {
          while (true) {
            var line = reader.readLine();
            if (line == null) {
              break;
            }

            var edge = Edge.fromCSV(line);
            Edges.checkIncreasing(edge.sId, edge.dId, prevSId, prevDId);
            prevSId = edge.sId;
            prevDId = edge.dId;

            SQL.insert(edge, preparedInsert);
            inserted++;

            var nanoTime = System.nanoTime();
            if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - lastPrint) >= 10) {
              Edges.printProgress(lastInserted, inserted, Edges.numEdgesInCSVFile, startNanos);
              lastInserted = inserted;
              lastPrint = nanoTime;
            }
          }

          // TODO: create indices

          var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
          var totalPerSec = inserted / sec;
          System.out.println("Inserted " + inserted + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
        }
      }
    }
  }
}
