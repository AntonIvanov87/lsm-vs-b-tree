package aivanov.cassandra;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
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

import aivanov.Edge;
import aivanov.Edges;

class CheckAllInCassandra {

  public static void main(String[] args) throws IOException {
    var cassandraDaemon = Cassandra.start();
    try {
      var startNanos = System.nanoTime();
      var processed = 0;
      var prevProcessed = 0;
      var prevPrintNanos = System.nanoTime();

      var preparedSelect = prepareSelect();
      try (var lines = Files.newBufferedReader(Edges.csvFile)) {
        while (true) {
          var line = lines.readLine();
          if (line == null) {
            break;
          }
          var edge = Edge.fromCSV(line);
          checkEdgeInserted(edge, preparedSelect);
          processed++;

          var nanoTime = System.nanoTime();
          if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
            Edges.printProgress(prevProcessed, processed, Edges.numEdgesInCSVFile, startNanos);
            prevProcessed = processed;
            prevPrintNanos = nanoTime;
          }
        }
      }

      var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
      var totalPerSec = processed / sec;
      System.out.println("Inserted " + processed + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");

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

  private static void checkEdgeInserted(Edge edge, ResultMessage.Prepared preparedSelect) {
    var resp = new ExecuteMessage(
        preparedSelect.statementId,
        preparedSelect.resultMetadataId,
        QueryOptions.forInternalCalls(List.of(
            LongSerializer.instance.serialize(edge.sId),
            LongSerializer.instance.serialize(edge.dId)
        ))
    ).execute(
        new QueryState(ClientState.forInternalCalls("test")),
        System.nanoTime()
    );
    if (!(resp instanceof ResultMessage.Rows)) {
      throw new IllegalStateException("Unexpected SELECT edge response: " + resp);
    }
    var rows = (ResultMessage.Rows) resp;
    if (rows.result.isEmpty()) {
      throw new IllegalStateException("Edge (" + edge.sId + ", " + edge.dId + ") has not been found after insert");
    }
    var row = rows.result.rows.get(0);

    var rowSId = row.get(0).getLong();
    if (rowSId != edge.sId) {
      throw new IllegalStateException("s_id of selected edge (" + rowSId + ") is not equal to required s_id (" + edge.sId + ")");
    }

    var rowDId = row.get(1).getLong();
    if (rowDId != edge.dId) {
      throw new IllegalStateException("s_id of selected edge (" + rowDId + ") is not equal to required s_id (" + edge.dId + ")");
    }

    var rowASt = row.get(2).get();
    if (rowASt != edge.st) {
      throw new IllegalStateException("a_st of selected edge (" + rowASt + ") is not equal to required st (" + edge.st + ")");
    }
  }

}
