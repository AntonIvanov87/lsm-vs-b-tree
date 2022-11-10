package aivanov.edge;

import com.google.common.hash.BloomFilter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Edges {

    public static final Path binFile = Path.of("source-data", "g1_edges_idx_fwd_123_2022_10_12_12_03_54.edg");
    private static final Path bloomFilterFile = Path.of("bloom-filter-data", "bloom-filter");

    public static final long numEdgesInFile = 283686607;
    public static final long minEdgeId = 4801L;
    public static final long maxEdgeId = 1580167090472120320L;

    public static void storeBloomFilter(BloomFilter<EdgeKey> bloomFilter) throws IOException {
        try (var bloomFilterOutStream = new BufferedOutputStream(Files.newOutputStream(bloomFilterFile))) {
            bloomFilter.writeTo(bloomFilterOutStream);
        }
    }

    public static BloomFilter<EdgeKey> loadBloomFilter() throws IOException {
        try(var bloomFilterInStream = new BufferedInputStream(Files.newInputStream(Edges.bloomFilterFile))) {
            return BloomFilter.readFrom(bloomFilterInStream, EdgeKey.funnel);
        }
    }

    private Edges() {
    }
}
