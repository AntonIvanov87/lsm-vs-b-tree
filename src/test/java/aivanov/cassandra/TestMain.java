package aivanov.cassandra;

import java.io.IOException;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

class TestMain {
  public static void main(String[] args) throws IOException, InterruptedException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra();
  }
}
