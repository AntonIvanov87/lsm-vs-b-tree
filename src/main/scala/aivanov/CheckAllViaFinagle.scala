package aivanov

import aivanov.thriftscala.EdgeService
import com.twitter.finagle.ThriftMux
import com.twitter.util.Await

import java.nio.file.Files
import java.util.concurrent.TimeUnit

object CheckAllViaFinagle {

  def main(args: Array[String]): Unit = {
    val edgeClient = EdgeService.MethodPerEndpoint(
      ThriftMux.client
        .methodBuilder("127.0.0.1:9090")
        .servicePerEndpoint[EdgeService.ServicePerEndpoint]
    )
    try {
      val startNanos = System.nanoTime
      var processed = 0
      var prevProcessed = 0
      var prevPrintNanos = System.nanoTime

      val lines = Files.newBufferedReader(Edges.csvFile)
      try {
        var line = lines.readLine()
        while (line != null) {
          val edge = Edge.fromCSV(line)
          checkEdgeInserted(edge, edgeClient)
          processed += 1

          val nanoTime = System.nanoTime
          if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
            Edges.printProgress(
              prevProcessed,
              processed,
              Edges.numEdgesInCSVFile,
              startNanos
            )
            prevProcessed = processed
            prevPrintNanos = nanoTime
          }

          line = lines.readLine()
        }

      } finally {
        lines.close()
      }

      val sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime - startNanos)
      val totalPerSec = processed / sec
      System.out.println(
        "Inserted " + processed + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec"
      )

    } finally {
      Await.result(edgeClient.asClosable.close())
    }
  }

  private def checkEdgeInserted(edge: Edge,
                                edgeClient: EdgeService.MethodPerEndpoint,
  ): Unit = {
    val response = Await.result(edgeClient.fetchEdge(edge.sId, edge.dId))
    if (response.edge.isEmpty)
      throw new IllegalStateException(
        "Edge (" + edge.sId + ", " + edge.dId + ") has not been found"
      )
    if (response.edge.get.st != edge.st)
      throw new IllegalStateException(
        "a_st of selected edge (" + response.edge.get + ") is not equal to required st (" + edge.st + ")"
      )
  }

}
