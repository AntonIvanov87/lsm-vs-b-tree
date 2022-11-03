namespace java aivanov.thriftjava
#@namespace scala aivanov.thriftscala

struct Edge {
  1: i64 sId
  2: i64 dId
  3: i8 st
  4: i64 ut
  5: i64 pos
  6: i8 ann
}

struct FetchEdgeResponse {
    1: optional Edge edge
}

service EdgeService {
  FetchEdgeResponse fetchEdge(1: i64 sId, 2: i64 dId)
}