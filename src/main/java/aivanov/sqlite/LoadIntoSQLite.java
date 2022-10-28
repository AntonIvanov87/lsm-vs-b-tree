package aivanov.sqlite;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import aivanov.Edge;
import aivanov.Edges;

class LoadIntoSQLite {

  private static final int edgesPerBatch = 1_000;

  public static void main(String[] args) throws SQLException, IOException {
    var startNanos = System.nanoTime();
    var inserted = 0L;
    var lastInserted = 0L;
    var lastPrint = System.nanoTime();

    try (var conn = SQLite.createConnection(false)) {
      createTable(conn);

      try (var preparedInsert = conn.prepareStatement(
          "INSERT INTO edges (s_id, s_st, s_ut, d_id, d_st, d_ut, st, ut, pos, ann, a_st, a_ut) VALUES (?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?)"
      )) {
        var prevSId = 0L;
        var prevDId = 0L;
        try (var reader = Files.newBufferedReader(Edges.csvFile)) {
          conn.setAutoCommit(false);
          while (true) {
            var line = reader.readLine();
            if (line == null) {
              break;
            }

            var edge = Edge.fromCSV(line);
            Edges.checkIncreasing(edge.sId, edge.dId, prevSId, prevDId);
            prevSId = edge.sId;
            prevDId = edge.dId;

            insert(edge, preparedInsert);
            inserted++;
            if (inserted % edgesPerBatch == 0) {
              conn.commit();
              checkEdgeInserted(edge, conn);
              conn.setAutoCommit(false);
            }

            var nanoTime = System.nanoTime();
            if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - lastPrint) >= 10) {
              Edges.printProgress(lastInserted, inserted, Edges.numEdgesInCSVFile, startNanos);
              lastInserted = inserted;
              lastPrint = nanoTime;
            }
          }

          conn.commit();

          // TODO: create indices

          var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
          var totalPerSec = inserted / sec;
          System.out.println("Inserted " + inserted + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
        }
      }
    }
  }

  private static void createTable(Connection conn) throws SQLException {
    try (var statement = conn.createStatement()) {
      statement.execute(
          "CREATE TABLE edges(" +
              "s_id BIGINT NOT NULL, " +
              "s_st TINYINT NOT NULL, " +
              "s_ut BIGINT NOT NULL, " +
              "d_id BIGINT NOT NULL, " +
              "d_st TINYINT NOT NULL, " +
              "d_ut BIGINT NOT NULL, " +
              "st TINYINT NOT NULL, " +
              "ut BIGINT NOT NULL, " +
              "pos BIGINT NOT NULL, " +
              "ann TINYINT UNSIGNED NOT NULL, " +
              "a_st TINYINT, " +
              "a_ut BIGINT, " +
              "r_ut BIGINT, " +
              "PRIMARY KEY (s_id, d_id) " +
              ")"
      );
    }
  }

  private static void insert(Edge edge, PreparedStatement preparedInsert) throws SQLException {
    preparedInsert.setLong(1, edge.sId);
    preparedInsert.setByte(2, (byte) 0);
    preparedInsert.setLong(3, edge.ut);

    preparedInsert.setLong(4, edge.dId);
    preparedInsert.setByte(5, (byte) 0);
    preparedInsert.setLong(6, edge.ut);

    preparedInsert.setByte(7, edge.st);
    preparedInsert.setLong(8, edge.ut);
    preparedInsert.setLong(9, edge.pos);
    preparedInsert.setByte(10, edge.ann);

    preparedInsert.setByte(11, edge.st);
    preparedInsert.setLong(12, edge.ut);

    var insertResult = preparedInsert.executeUpdate();
    if (insertResult != 1) {
      throw new IllegalStateException("Unexpected executeUpdate result: " + insertResult);
    }
  }

  private static void checkEdgeInserted(Edge edge, Connection conn) throws SQLException {
    try (var statement = conn.createStatement()) {
      try (var rs = statement.executeQuery("SELECT s_id, d_id, a_st FROM edges WHERE s_id = " + edge.sId + " AND d_id = " + edge.dId)) {
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
}
