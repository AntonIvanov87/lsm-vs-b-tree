package aivanov.mysql;

import aivanov.edge.BinFileAsArray;
import com.twitter.finagle.mysql.Client;
import com.twitter.util.Await;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class CheckRandomExistingViaFinagleMySQL {

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(CheckRandomExistingViaFinagleMySQL.class.getSimpleName())
                .forks(1)
                .threads(3)
                .build();
        new Runner(opt).run();
    }

    private Client finagleMySQLClient;
    private ThreadLocal<BinFileAsArray> edges;

    @Setup(Level.Trial)
    public void setup() {
        finagleMySQLClient = MySQL.createFinagleMySQLClient();
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
        Await.result(finagleMySQLClient.close());
    }

    @Benchmark
    public long benchmark() throws Exception {
        var randomIndex = ThreadLocalRandom.current().nextLong(0, edges.get().length);
        var randomEdge = edges.get().get(randomIndex);
        var fetchedEdge = MySQL.selectEdge(randomEdge.sId, randomEdge.dId, finagleMySQLClient);
        if (fetchedEdge.isEmpty()) {
            throw new IllegalStateException(randomEdge + " not found in DB");
        }
        return fetchedEdge.get().sId();
    }

}
