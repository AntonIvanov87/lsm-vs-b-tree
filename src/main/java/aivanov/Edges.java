package aivanov;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class Edges {

  public static final Path csvFile = Path.of("g1_edges_idx_fwd_123_2022_10_12_12_03_54.csv");
  public static final long numEdgesInCSVFile = 283686607;
  public static final long minEdgeId = 4801L;
  public static final long maxEdgeId = 1580167090472120320L;

  public static void checkIncreasing(long sId, long dId, long prevSId, long prevDId) {
    if (sId < prevSId) {
      throw new IllegalStateException("Current s_id (" + sId + ") < previous s_id (" + prevSId + ")");
    }
    if (prevSId == sId && dId <= prevDId) {
      throw new IllegalStateException("Current d_id (" + dId + ") <= previous d_id (" + prevDId + ")");
    }
  }

  public static void printProgress(long prevProcessed, long curProcessed, long totalEdges, long startNanos) {
    var percent = 100 * curProcessed / totalEdges;
    var totalPerSec = curProcessed / TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
    var curPerSec = (curProcessed - prevProcessed) / 10;
    System.out.println("Processed " + curProcessed + " edges (" + percent + "%) , average speed " + totalPerSec + " / sec, current speed " + curPerSec + " / sec");
  }

}
