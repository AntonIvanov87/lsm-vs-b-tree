package aivanov.sqlite;

import aivanov.SQL;
import aivanov.edge.Edges;
import com.zaxxer.hikari.HikariDataSource;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class CheckRandomInSQLite {

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(CheckRandomInSQLite.class.getSimpleName())
                .forks(1)
                .threads(3)
                .build();
        new Runner(opt).run();
    }

    private HikariDataSource dataSource;

    @Setup(Level.Trial)
    public void setup() {
        dataSource = SQLite.createDataSource(true);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        dataSource.close();
    }

    @Benchmark
    public boolean benchmark() throws SQLException, ExecutionException, InterruptedException {
        var random = ThreadLocalRandom.current();
        var sId = random.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        var dId = random.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        return SQL.selectEdge(sId, dId, dataSource).isPresent();
    }

}
