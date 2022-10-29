package aivanov;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQL {

  public static void createTable(Connection conn) throws SQLException {
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

  public static final String insertQuery = "INSERT INTO edges (s_id, s_st, s_ut, d_id, d_st, d_ut, st, ut, pos, ann, a_st, a_ut) VALUES (?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?)";

  public static void insert(Edge edge, PreparedStatement preparedInsert) throws SQLException {
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

  public static PreparedStatement prepareSelect(Connection conn) throws SQLException {
    return conn.prepareStatement(
        "SELECT * FROM edges WHERE s_id = ? AND d_id = ?"
    );
  }
  public static boolean edgeExists(long sId, long dId, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setLong(1, sId);
    preparedStatement.setLong(2, dId);
    try (var rs = preparedStatement.executeQuery()) {
      return rs.next();
    }
  }

  public static void checkEdge(Edge edge, PreparedStatement preparedStatement) throws SQLException {
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

  private SQL() {

  }
}
