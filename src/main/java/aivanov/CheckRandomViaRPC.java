package aivanov;

import aivanov.edge.Edges;
import aivanov.thriftscala.EdgeService;
import com.twitter.util.Await;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class CheckRandomViaRPC {

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(CheckRandomViaRPC.class.getSimpleName())
                .forks(1)
                .threads(3)
                .build();
        new Runner(opt).run();
    }

    private EdgeService.MethodPerEndpoint edgeClient;

    @Setup(Level.Trial)
    public void setup() {
        edgeClient = FinagleHelper.createEdgeClient();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        Await.result(edgeClient.asClosable().close());
    }

    @Benchmark
    public boolean benchmark() throws Exception {
        var random = ThreadLocalRandom.current();
        var sId = random.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        var dId = random.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        return Await.result(edgeClient.fetchEdge(sId, dId)).edge().isDefined();
    }

}
