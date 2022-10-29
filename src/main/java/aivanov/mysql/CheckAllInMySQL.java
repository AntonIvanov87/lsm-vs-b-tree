package aivanov.mysql;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import aivanov.Edge;
import aivanov.Edges;
import aivanov.SQL;

class CheckAllInMySQL {

  public static void main(String[] args) throws SQLException, IOException {
    var startNanos = System.nanoTime();
    var checked = 0L;
    var lastChecked = 0L;
    var lastPrint = System.nanoTime();

    try (var conn = MySQL.createConnection()) {
      try (var preparedSelect = SQL.prepareSelect(conn)) {
        try (var reader = Files.newBufferedReader(Edges.csvFile)) {
          while (true) {
            var line = reader.readLine();
            if (line == null) {
              break;
            }

            var edge = Edge.fromCSV(line);
            SQL.checkEdge(edge, preparedSelect);
            checked++;

            var nanoTime = System.nanoTime();
            if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - lastPrint) >= 10) {
              Edges.printProgress(lastChecked, checked, Edges.numEdgesInCSVFile, startNanos);
              lastChecked = checked;
              lastPrint = nanoTime;
            }
          }

          var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
          var totalPerSec = checked / sec;
          System.out.println("Checked " + checked + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
        }
      }
    }
  }
}
