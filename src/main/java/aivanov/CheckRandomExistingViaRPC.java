package aivanov;

import aivanov.edge.BinFileAsArray;
import aivanov.thriftscala.EdgeService;
import com.twitter.util.Await;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class CheckRandomExistingViaRPC {

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(CheckRandomExistingViaRPC.class.getSimpleName())
                .forks(1)
                .threads(3)
                .build();
        new Runner(opt).run();
    }

    private EdgeService.MethodPerEndpoint edgeClient;
    private ThreadLocal<BinFileAsArray> edges;

    @Setup(Level.Trial)
    public void setup() {
        edgeClient = FinagleHelper.createEdgeClient();
        edges = ThreadLocal.withInitial(() -> {
            try {
                return new BinFileAsArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        Await.result(edgeClient.asClosable().close());
    }

    @Benchmark
    public long benchmark() throws Exception {
        var randomIndex = ThreadLocalRandom.current().nextLong(0, edges.get().length);
        var randomEdge = edges.get().get(randomIndex);
        var fetchedEdge = Await.result(
                edgeClient.fetchEdge(randomEdge.sId, randomEdge.dId)
        ).edge();
        if (fetchedEdge.isEmpty()) {
            throw new IllegalStateException(randomEdge + " not found in DB");
        }
        return fetchedEdge.get().sId();
    }

}
