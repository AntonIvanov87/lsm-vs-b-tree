package aivanov;

import aivanov.edge.Edge;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

public class SQL {

    public static void createTable(Connection conn) throws SQLException {
        try (var statement = conn.createStatement()) {
            statement.execute("CREATE TABLE edges(" + "s_id BIGINT NOT NULL, " + "s_st TINYINT NOT NULL, " + "s_ut BIGINT NOT NULL, " + "d_id BIGINT NOT NULL, " + "d_st TINYINT NOT NULL, " + "d_ut BIGINT NOT NULL, " + "st TINYINT NOT NULL, " + "ut BIGINT NOT NULL, " + "pos BIGINT NOT NULL, " + "ann TINYINT UNSIGNED NOT NULL, " + "a_st TINYINT, " + "a_ut BIGINT, " + "r_ut BIGINT, " + "PRIMARY KEY (s_id, d_id) " + ")");
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

    public static final String selectQuery = "SELECT a_st, a_ut, pos, ann FROM edges WHERE s_id = ? AND d_id = ?";

    // private static final ForkJoinPool executor = (ForkJoinPool) Executors.newWorkStealingPool(2048);

    public static Optional<aivanov.thriftscala.Edge> selectEdge(long sId, long dId, DataSource dataSource) throws SQLException {
        // if (executor.getQueuedSubmissionCount() > 2048) {
        //     throw new RejectedExecutionException("Too many queued tasks");
        // }
        // return executor.submit(() -> {
            try (var conn = dataSource.getConnection()) {
                var preparedSelect = conn.prepareStatement(selectQuery);
                preparedSelect.setLong(1, sId);
                preparedSelect.setLong(2, dId);

                try (var rs = preparedSelect.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.<aivanov.thriftscala.Edge>empty();
                    }
                    return Optional.of(aivanov.thriftscala.Edge.apply(sId, dId, rs.getByte("a_st"), rs.getLong("a_ut"), rs.getLong("pos"), rs.getByte("ann")));
                }
            }
        // }).get();
    }

    private SQL() {
    }
}
