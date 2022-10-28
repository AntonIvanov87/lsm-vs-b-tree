package aivanov.cassandra;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import aivanov.Edge;
import aivanov.Edges;

import static java.util.Map.entry;

class LoadIntoCassandra {

  private static final int edgesInBatch = 10_000_000;
  private static final Path tempSSTablesDir = Paths.get("temp-sstables", "test", "edges");

  public static void main(String[] args) throws IOException {
    var cassandraDaemon = Cassandra.start();
    try {
      var startNanos = System.nanoTime();
      var parsed = 0;
      var prevParsed = 0;
      var prevPrintNanos = System.nanoTime();

      Cassandra.createKeyspace();
      Cassandra.createTable();

      ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists("test", "edges");

      // TODO: proper temp dir
      Files.createDirectories(tempSSTablesDir);
      deleteTempSSTables();
      var sstWriterBuilder = CQLSSTableWriter
          .builder()
          .inDirectory(tempSSTablesDir.toFile())
          .forTable(Cassandra.tableSchema)
          .using(Cassandra.insertQuery)
          .withBufferSizeInMB(256);
      var sstWriter = sstWriterBuilder.build();
      try {
        Edge anEdge = null;
        var prevSId = -1L;
        var prevDId = -1L;
        try (var lines = Files.newBufferedReader(Edges.csvFile)) {
          while (true) {
            var line = lines.readLine();
            if (line == null) {
              break;
            }
            var edge = Edge.fromCSV(line);

            Edges.checkIncreasing(edge.sId, edge.dId, prevSId, prevDId);
            prevSId = edge.sId;
            prevDId = edge.dId;

            appendEdgeToTempSSTable(edge, sstWriter);
            parsed++;
            if (parsed % edgesInBatch == 0) {
              sstWriter.close();
              sstWriter = null;

              importTempSSTables(cfs);
              if (anEdge == null) {
                anEdge = edge;
              } else {
                checkEdgeInserted(anEdge);
              }
              checkEdgeInserted(edge);

              deleteTempSSTables();
              sstWriter = sstWriterBuilder.build();
            }

            var nanoTime = System.nanoTime();
            if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
              Edges.printProgress(prevParsed, parsed, Edges.numEdgesInCSVFile, startNanos);
              prevParsed = parsed;
              prevPrintNanos = nanoTime;
            }
          }
        }

        sstWriter.close();
        sstWriter = null;
        importTempSSTables(cfs);

      } finally {
        if (sstWriter != null) {
          sstWriter.close();
        }
        deleteTempSSTables();
      }

      // TODO: create indices

      var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
      var totalPerSec = parsed / sec;
      System.out.println("Inserted " + parsed + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
    } finally {
      cassandraDaemon.deactivate();
    }
  }

  private static void appendEdgeToTempSSTable(Edge edge, CQLSSTableWriter tempFileWriter) throws IOException {
    tempFileWriter.addRow(
        Map.ofEntries(
            entry("s_id", edge.sId),
            entry("s_st", (byte) 0),
            entry("s_ut", edge.ut),
            entry("d_id", edge.dId),
            entry("d_st", (byte) 0),
            entry("d_ut", edge.ut),
            entry("st", edge.st),
            entry("ut", edge.ut),
            entry("pos", edge.pos),
            entry("ann", edge.ann),
            entry("a_st", edge.st),
            entry("a_ut", edge.ut)
        )
    );
  }

  private static void importTempSSTables(ColumnFamilyStore cfs) {
    var verifySSTables = false;
    var verifyTokens = false;
    var extendedVerify = false;
    // TODO: try copy?
    var copyData = false;
    cfs.importNewSSTables(Set.of(tempSSTablesDir.toString()), false, false, verifySSTables, verifyTokens, false, extendedVerify, copyData);
  }

  private static void checkEdgeInserted(Edge edge) {
    var resp = new QueryMessage("SELECT s_id, d_id, a_st from test.edges WHERE s_id = " + edge.sId + " AND d_id = " + edge.dId, QueryOptions.DEFAULT).execute(new QueryState(ClientState.forInternalCalls("test")), System.nanoTime());
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

  private static void deleteTempSSTables() throws IOException {
    try (var sstables = Files.list(tempSSTablesDir)) {
      sstables.forEachOrdered(sst -> {
        try {
          Files.delete(sst);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
}
