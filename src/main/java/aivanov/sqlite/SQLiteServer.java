package aivanov.sqlite;

import aivanov.SQL;
import aivanov.thriftscala.Edge;
import aivanov.thriftscala.EdgeService;
import aivanov.thriftscala.FetchEdgeResponse;
import com.twitter.finagle.ThriftMux;
import com.twitter.util.Await;
import com.twitter.util.Future;
import scala.Option;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

class SQLiteServer {

    public static void main(String[] args) throws Exception {
        try (var conn = SQLite.createConnection(true)) {
            var server = ThriftMux.server().serveIface("127.0.0.1:9090", new EdgeServiceImpl(conn));
            Await.result(server.closeFuture());
        }
    }

    private static class EdgeServiceImpl implements EdgeService.MethodPerEndpoint {

        private final PreparedStatement preparedSelect;

        EdgeServiceImpl(Connection connection) throws SQLException {
            this.preparedSelect = SQL.prepareSelect(connection);
        }

        @Override
        public Future<FetchEdgeResponse> fetchEdge(long sId, long dId) {
            Optional<Edge> edgeOpt;
            try {
                edgeOpt = SQL.selectEdge(sId, dId, preparedSelect);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return Future.value(FetchEdgeResponse.apply(Option.apply(edgeOpt.orElse(null))));
        }
    }

}
