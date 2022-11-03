package aivanov

import aivanov.thriftscala.EdgeService
import com.twitter.finagle.ThriftMux
import com.twitter.util.Await

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

object CheckRandomViaFinagle {

  private val attempts = 10_000_000

  def main(args: Array[String]): Unit = {
    val edgeClient = EdgeService.MethodPerEndpoint(
      ThriftMux.client
        .methodBuilder("127.0.0.1:9090")
        .servicePerEndpoint[EdgeService.ServicePerEndpoint]
    )
    try {
      val startNanos = System.nanoTime
      var processed = 0
      var exists = 0
      var prevProcessed = 0
      var prevPrintNanos = System.nanoTime

      val rand = ThreadLocalRandom.current
      while (processed < attempts) {
        val sId = rand.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1)
        val dId = rand.nextLong(Edges.minEdgeId, Edges.maxEdgeId + 1)
        if (Await.result(edgeClient.fetchEdge(sId, dId)).edge.isDefined) {
          exists += 1
        }
        processed += 1

        val nanoTime = System.nanoTime()
        if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
          Edges.printProgress(prevProcessed, processed, attempts, startNanos)
          System.out.println("Hit rate = " + (100.0 * exists / processed) + "%")
          prevProcessed = processed
          prevPrintNanos = nanoTime
        }
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

}
