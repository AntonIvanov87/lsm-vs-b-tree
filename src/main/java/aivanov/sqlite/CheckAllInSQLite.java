package aivanov.sqlite;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import aivanov.Edge;
import aivanov.Edges;

class CheckAllInSQLite {

  public static void main(String[] args) throws SQLException, IOException {
    var startNanos = System.nanoTime();
    var checked = 0L;
    var lastChecked = 0L;
    var lastPrint = System.nanoTime();

    try (var conn = SQLite.createConnection(true)) {
      try (var preparedSelect = conn.prepareStatement(
          "SELECT * FROM edges WHERE s_id = ? AND d_id = ?"
      )) {
        try (var reader = Files.newBufferedReader(Edges.csvFile)) {
          while (true) {
            var line = reader.readLine();
            if (line == null) {
              break;
            }

            var edge = Edge.fromCSV(line);
            checkEdge(edge, preparedSelect);
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

  private static void checkEdge(Edge edge, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setLong(1, edge.sId);
    preparedStatement.setLong(2, edge.dId);
      try (var rs = preparedStatement.executeQuery()) {
        if (!rs.next()) {
          throw new IllegalStateException("Have not found an inserted edge (" + edge.sId + ", " + edge.dId + ")");
        }

        var rowSId = rs.getLong("s_id");
        if (rowSId != edge.sId) {
          throw new IllegalStateException("s_id of selected edge (" + rowSId + ") is not equal to required s_id (" + edge.sId + ")");
        }

        var rowDId = rs.getLong("d_id");
        if (rowDId != edge.dId) {
          throw new IllegalStateException("s_id of selected edge (" + rowDId + ") is not equal to required s_id (" + edge.dId + ")");
        }

        var rowASt = rs.getByte("a_st");
        if (rowASt != edge.st) {
          throw new IllegalStateException("a_st of selected edge (" + rowASt + ") is not equal to required st (" + edge.st + ")");
        }
      }

  }
}
