package aivanov.mysql;

import aivanov.EdgeServiceSQLImpl;
import com.twitter.finagle.ThriftMux;
import com.twitter.util.Await;

class MySQLRPCServer {

    public static void main(String[] args) throws Exception {
        try (var dataSource = MySQL.createDataSource(true)) {
            var server = ThriftMux.server().serveIface("127.0.0.1:9090", new EdgeServiceSQLImpl(dataSource));
            Await.result(server.closeFuture());
        }
    }

}
