package aivanov.cassandra;

import aivanov.edge.BinFileAsArray;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class CheckRandomExistingInCassandra {

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(CheckRandomExistingInCassandra.class.getSimpleName())
                .forks(1)
                .threads(3)
                .build();
        new Runner(opt).run();
    }

    private CassandraDaemon cassandraDaemon;
    private ResultMessage.Prepared preparedSelect;
    private ThreadLocal<BinFileAsArray> edges;

    @Setup(Level.Trial)
    public void setup() {
        cassandraDaemon = Cassandra.start();
        preparedSelect = Cassandra.prepareSelect();
        edges = ThreadLocal.withInitial(() -> {
            try {
                return new BinFileAsArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cassandraDaemon.stop();
    }

    @Benchmark
    public long benchmark() throws IOException {
        var randomIndex = ThreadLocalRandom.current().nextLong(0, edges.get().length);
        var randomEdge = edges.get().get(randomIndex);
        var fetchedEdge = Cassandra.selectEdge(randomEdge.sId, randomEdge.dId, preparedSelect);
        if (fetchedEdge.isEmpty()) {
            throw new IllegalStateException(randomEdge + " not found in DB");
        }
        return fetchedEdge.get().sId();
    }

}
