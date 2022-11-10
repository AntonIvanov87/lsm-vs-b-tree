package aivanov.sqlite;

import aivanov.SQL;
import aivanov.edge.BinFileAsArray;
import com.zaxxer.hikari.HikariDataSource;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class CheckRandomExistingInSQLite {

    public static void main(String[] args) throws RunnerException {
        var opt = new OptionsBuilder()
                .include(CheckRandomExistingInSQLite.class.getSimpleName())
                .forks(1)
                .threads(3)
                .build();
        new Runner(opt).run();
    }

    private HikariDataSource dataSource;
    private ThreadLocal<BinFileAsArray> edges;

    @Setup(Level.Trial)
    public void setup() {
        dataSource = SQLite.createDataSource(true);
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
        dataSource.close();
    }

    @Benchmark
    public long benchmark() throws SQLException, ExecutionException, InterruptedException, IOException {
        var randomIndex = ThreadLocalRandom.current().nextLong(0, edges.get().length);
        var randomEdge = edges.get().get(randomIndex);
        var fetchedEdge = SQL.selectEdge(randomEdge.sId, randomEdge.dId, dataSource);
        if (fetchedEdge.isEmpty()) {
            throw new IllegalStateException(randomEdge + " not found in DB");
        }
        return fetchedEdge.get().sId();
    }

}
