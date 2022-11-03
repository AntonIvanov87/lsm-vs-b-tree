package aivanov.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class MySQL {

  static Connection createConnection() throws SQLException {
    // TODO: options
    // TODO: unix domain socket
    //return DriverManager.getConnection("jdbc:mysql://localhost/test?user=root");
    return DriverManager.getConnection("jdbc:mysql://localhost/test?user=root&socketFactory=org.newsclub.net.mysql.AFUNIXDatabaseSocketFactory&junixsocket.file=/tmp/mysql.sock&sslMode=DISABLED");
  }

  static void recreateDatabase() throws SQLException {
    try(var conn = DriverManager.getConnection("jdbc:mysql://localhost?user=root")) {
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

  private MySQL() {
  }
}
