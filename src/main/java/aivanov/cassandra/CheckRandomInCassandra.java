package aivanov.cassandra;

import aivanov.edge.Edges;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class CheckRandomInCassandra {

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(CheckRandomInCassandra.class.getSimpleName())
                .forks(1)
                .threads(3)
                .build();
        new Runner(opt).run();
    }

    private CassandraDaemon cassandraDaemon;
    private ResultMessage.Prepared preparedSelect;

    @Setup(Level.Trial)
    public void setup() {
        cassandraDaemon = Cassandra.start();
        preparedSelect = Cassandra.prepareSelect();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cassandraDaemon.stop();
    }

    @Benchmark
    public boolean benchmark() {
        var random = ThreadLocalRandom.current();
        var sId = random.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        var dId = random.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        return Cassandra.selectEdge(sId, dId, preparedSelect).isPresent();
    }

}
