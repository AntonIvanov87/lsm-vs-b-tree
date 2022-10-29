package aivanov.sqlite;

import java.sql.Connection;
import java.sql.SQLException;

import org.sqlite.SQLiteConfig;

class SQLite {

  static Connection createConnection(boolean readOnly) throws SQLException {
    var sqlLiteConf = new SQLiteConfig();
    sqlLiteConf.setJournalMode(SQLiteConfig.JournalMode.WAL);
    sqlLiteConf.setCacheSize(-131072);
    sqlLiteConf.setReadOnly(readOnly);
    return sqlLiteConf.createConnection("jdbc:sqlite:sqlite.db");
  }

  private SQLite() {
  }
}
