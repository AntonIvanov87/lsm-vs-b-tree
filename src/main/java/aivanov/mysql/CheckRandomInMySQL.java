package aivanov.mysql;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import aivanov.Edges;
import aivanov.SQL;

class CheckRandomInMySQL {

  private static final int attempts = 10_000_000;

  public static void main(String[] args) throws SQLException {
    var startNanos = System.nanoTime();
    var checked = 0L;
    var exists = 0L;
    var lastChecked = 0L;
    var lastPrint = System.nanoTime();

    // TODO: read-only
    try (var conn = MySQL.createConnection()) {
      try (var preparedSelect = SQL.prepareSelect(conn)) {
        var rand = ThreadLocalRandom.current();
        while (checked < attempts) {
          var sId = rand.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
          var dId = rand.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
          if (SQL.edgeExists(sId, dId, preparedSelect)) {
            exists++;
          }
          checked++;

          var nanoTime = System.nanoTime();
          if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - lastPrint) >= 10) {
            Edges.printProgress(lastChecked, checked, attempts, startNanos);
            System.out.println("Hit rate = " + (100.0 * exists / checked) + "%");
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
