package aivanov.sqlite;

import com.zaxxer.hikari.HikariDataSource;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

class SQLite {

    static HikariDataSource createDataSource(boolean readOnly) {
        var sqliteConf = new SQLiteConfig();
        sqliteConf.setJournalMode(SQLiteConfig.JournalMode.WAL);
        sqliteConf.setCacheSize(-131072);
        sqliteConf.setReadOnly(readOnly);

        var sqliteDataSource = new SQLiteDataSource(sqliteConf);
        sqliteDataSource.setUrl("jdbc:sqlite:sqlite-storage/sqlite.db");

        var dataSource = new HikariDataSource();
        dataSource.setDataSource(sqliteDataSource);
        dataSource.setMaximumPoolSize(Runtime.getRuntime().availableProcessors() * 2);
        dataSource.setReadOnly(readOnly);
        dataSource.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

        return dataSource;
    }

    private SQLite() {
    }
}
