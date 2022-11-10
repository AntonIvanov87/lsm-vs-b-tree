package aivanov;

import aivanov.thriftscala.Edge;
import aivanov.thriftscala.EdgeService;
import aivanov.thriftscala.FetchEdgeResponse;
import com.twitter.util.Future;
import scala.Option;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Optional;

public class EdgeServiceSQLImpl implements EdgeService.MethodPerEndpoint {

    private final DataSource dataSource;
    // private final BloomFilter<EdgeKey> bloomFilter;

    public EdgeServiceSQLImpl(DataSource dataSource) throws IOException {
        this.dataSource = dataSource;
        // this.bloomFilter = Edges.loadBloomFilter();
    }

    private static final Future<FetchEdgeResponse> emptyResponse = Future.value(new FetchEdgeResponse.Immutable(Option.empty()));

    @Override
    public Future<FetchEdgeResponse> fetchEdge(long sId, long dId) {
        // if (!bloomFilter.mightContain(new EdgeKey(sId, dId))) {
        //     return emptyResponse;
        // }
        Optional<Edge> edgeOpt;
        try {
            edgeOpt = SQL.selectEdge(sId, dId, dataSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (edgeOpt.isEmpty()) {
            return emptyResponse;
        }
        return Future.value(
                FetchEdgeResponse.apply(
                        Option.apply(edgeOpt.get())
                )
        );
    }

}
