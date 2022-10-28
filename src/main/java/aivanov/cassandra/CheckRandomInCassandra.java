package aivanov.cassandra;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import aivanov.Edges;

class CheckRandomInCassandra {

  private static final int attempts = 10_000_000;

  public static void main(String[] args) {
    var cassandraDaemon = Cassandra.start();
    try {
      var startNanos = System.nanoTime();
      var processed = 0;
      var prevProcessed = 0;
      var prevPrintNanos = System.nanoTime();

      var exists = 0L;
      var preparedSelect = prepareSelect();
      var rand = ThreadLocalRandom.current();
      while (processed < attempts) {
        var sId = rand.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        var dId = rand.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1);
        if (edgeExists(sId, dId, preparedSelect)) {
          exists++;
        }
        processed++;

        var nanoTime = System.nanoTime();
        if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
          Edges.printProgress(prevProcessed, processed, attempts, startNanos);
          System.out.println("Hit rate = " + (100.0 * exists / processed) + "%");
          prevProcessed = processed;
          prevPrintNanos = nanoTime;
        }
      }

      var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
      var totalPerSec = processed / sec;
      System.out.println("Checked " + processed + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");

    } finally {
      cassandraDaemon.deactivate();
    }
  }

  private static ResultMessage.Prepared prepareSelect() {
    var prepResp = new PrepareMessage(
        "SELECT * FROM test.edges WHERE s_id = ? and d_id = ?",
        "test"
    ).execute(
        new QueryState(ClientState.forInternalCalls("test")),
        System.nanoTime()
    );
    if (!(prepResp instanceof ResultMessage.Prepared)) {
      throw new IllegalStateException("Unexpected prepare message result: " + prepResp);
    }
    return (ResultMessage.Prepared) prepResp;
  }

  private static boolean edgeExists(long sId, long dId, ResultMessage.Prepared preparedSelect) {
    var resp = new ExecuteMessage(
        preparedSelect.statementId,
        preparedSelect.resultMetadataId,
        QueryOptions.forInternalCalls(List.of(
            LongSerializer.instance.serialize(sId),
            LongSerializer.instance.serialize(dId)
        ))
    ).execute(
        new QueryState(ClientState.forInternalCalls("test")),
        System.nanoTime()
    );
    if (!(resp instanceof ResultMessage.Rows)) {
      throw new IllegalStateException("Unexpected SELECT edge response: " + resp);
    }
    var rows = (ResultMessage.Rows) resp;
    return !rows.result.isEmpty();
  }

}
