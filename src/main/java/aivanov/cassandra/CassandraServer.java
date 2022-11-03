package aivanov.cassandra;

import aivanov.thriftscala.EdgeService;
import aivanov.thriftscala.FetchEdgeResponse;
import com.twitter.finagle.ThriftMux;
import com.twitter.util.Await;
import com.twitter.util.Future;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.LoggerFactory;
import scala.Option;

class CassandraServer {

    public static void main(String[] args) throws Exception {
        var cassandraDaemon = Cassandra.start();
        try {
            var server = ThriftMux.server().serveIface("127.0.0.1:9090", new EdgeServiceImpl());
            Await.result(server.closeFuture());
        } catch (Exception e) {
            LoggerFactory.getLogger(CassandraServer.class).error("Unexpected error", e);
        } finally {
            cassandraDaemon.deactivate();
        }
    }

    private static class EdgeServiceImpl implements EdgeService.MethodPerEndpoint {

        private final ResultMessage.Prepared preparedSelect;

        EdgeServiceImpl() {
            this.preparedSelect = Cassandra.prepareSelect();
        }

        @Override
        public Future<FetchEdgeResponse> fetchEdge(long sId, long dId) {
            var edgeOpt = Cassandra.selectEdge(sId, dId, preparedSelect);
            return Future.value(
                    FetchEdgeResponse.apply(
                            Option.apply(edgeOpt.orElse(null))
                    )
            );
        }
    }

}
