package aivanov.mysql;

import aivanov.edge.Edges;
import com.twitter.finagle.mysql.Client;
import com.twitter.util.Await;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class CheckRandomViaFinagleMySQL {

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(CheckRandomViaFinagleMySQL.class.getSimpleName())
                .forks(1)
                .threads(3)
                .build();
        new Runner(opt).run();
    }

    private Client finagleMySQLClient;

    @Setup(Level.Trial)
    public void setup() {
        finagleMySQLClient = MySQL.createFinagleMySQLClient();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        Await.result(finagleMySQLClient.close());
    }

    @Benchmark
    public boolean benchmark() throws Exception {
        var random = ThreadLocalRandom.current();
        var sId = random.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        var dId = random.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        return MySQL.selectEdge(sId, dId, finagleMySQLClient).isPresent();
    }

}
