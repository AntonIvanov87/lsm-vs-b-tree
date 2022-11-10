package aivanov

import aivanov.thriftscala.EdgeService
import com.twitter.finagle.ThriftMux

object FinagleHelper {

  def createEdgeClient(): EdgeService.MethodPerEndpoint = {
    EdgeService.MethodPerEndpoint(
      ThriftMux.client
        .methodBuilder("127.0.0.1:9090")
        .servicePerEndpoint[EdgeService.ServicePerEndpoint]
    )
  }

}
