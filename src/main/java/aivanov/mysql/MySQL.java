package aivanov.mysql;

import aivanov.SQL;
import aivanov.thriftscala.Edge;
import com.twitter.finagle.Mysql;
import com.twitter.finagle.mysql.Client;
import com.twitter.finagle.mysql.Parameter;
import com.twitter.util.Await;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

class MySQL {

    static HikariDataSource createDataSource(boolean readOnly) {
        var dataSource = new HikariDataSource();
        // dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/test?cachePrepStmts=true");
        dataSource.setJdbcUrl("jdbc:mysql://localhost/test?cachePrepStmts=true&socketFactory=org.newsclub.net.mysql.AFUNIXDatabaseSocketFactory&junixsocket.file=/tmp/mysql.sock&sslMode=DISABLED");
        dataSource.setUsername("root");
        dataSource.setMaximumPoolSize(Runtime.getRuntime().availableProcessors() * 2);
        dataSource.setReadOnly(readOnly);
        dataSource.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        return dataSource;
    }

    static Client createFinagleMySQLClient() {
        return Mysql.client()
                .withNoOpportunisticTls()
                .withCredentials("root", null)
                .withDatabase("test")
                .withSessionPool().maxSize(Runtime.getRuntime().availableProcessors() * 2)
                .newRichClient("localhost:3306");
    }

    static void recreateDatabase() throws SQLException {
        try (var conn = DriverManager.getConnection("jdbc:mysql://localhost?user=root")) {
            try (var statement = conn.createStatement()) {
                statement.executeUpdate(
                        "DROP DATABASE IF EXISTS test"
                );
                statement.executeUpdate(
                        "CREATE DATABASE test"
                );
            }
        }
    }

    static Optional<Edge> selectEdge(long sId, long dId, Client client) throws Exception {
        var preparedSelect = client.prepare(SQL.selectQuery).asJava();
        var rsFuture = preparedSelect.read(Parameter.of(sId), Parameter.of(dId));
        var rs = Await.result(rsFuture);
        var rows = rs.rows();
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        var row = rows.apply(0);
        return Optional.of(
                aivanov.thriftscala.Edge.apply(
                        sId,
                        dId,
                        row.byteOrZero("a_st"),
                        row.longOrZero("a_ut"),
                        row.longOrZero("pos"),
                        row.byteOrZero("ann")
                )
        );
    }

    private MySQL() {
    }
}
