package aivanov.edge;

import aivanov.ProgressPrinter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;

class CSVToBinary {

    private static final Path csvFile = Path.of("source-data", "g1_edges_idx_fwd_123_2022_10_12_12_03_54.csv");

    public static void main(String[] args) throws IOException {
        var progressPrinter = new ProgressPrinter(Edges.numEdgesInFile);
        try (var csvReader = Files.newBufferedReader(csvFile)) {
            try (var binChannel = FileChannel.open(Edges.binFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                var checksum = new Adler32();
                var binBuffer = ByteBuffer.allocateDirect(8192);
                var prevSId = 0L;
                var prevDId = 0L;
                while (true) {
                    var line = csvReader.readLine();
                    if (line == null) {
                        break;
                    }
                    var edge = edgeFromCSV(line);

                    checkIncreasing(edge.sId, edge.dId, prevSId, prevDId);
                    prevSId = edge.sId;
                    prevDId = edge.dId;

                    binBuffer.putLong(edge.sId);
                    binBuffer.putLong(edge.dId);
                    binBuffer.put(edge.st);
                    binBuffer.putLong(edge.pos);
                    binBuffer.putLong(edge.ut);
                    binBuffer.put(edge.ann);
                    progressPrinter.incProgress();

                    if (binBuffer.remaining() < Edge.sizeInBytes) {
                        binBuffer.flip();
                        checksum.update(binBuffer);
                        binBuffer.flip();
                        binChannel.write(binBuffer);
                        binBuffer.clear();
                    }
                }

                if (binBuffer.position() > 0) {
                    binBuffer.flip();
                    checksum.update(binBuffer);
                }

                binBuffer.limit(binBuffer.capacity());
                binBuffer.putLong(checksum.getValue());
                binBuffer.flip();
                binChannel.write(binBuffer);
            }
        }

        if (progressPrinter.processed() != Edges.numEdgesInFile) {
            throw new IllegalStateException("Processed " + progressPrinter.processed() + " edges != expected " + Edges.numEdgesInFile);
        }

        var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - progressPrinter.startNanos);
        var totalPerSec = progressPrinter.processed() / sec;
        System.out.println("Converted " + progressPrinter.processed() + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
    }

    public static Edge edgeFromCSV(String line) {
        var cells = line.split(",");
        var sId = Long.parseLong(cells[0]);
        var dId = Long.parseLong(cells[1]);
        var st = Byte.parseByte(cells[2]);
        var pos = Long.parseLong(cells[3]);
        var ut = Long.parseLong(cells[4]);
        var ann = Byte.parseByte(cells[5]);
        return new Edge(sId, dId, st, ut, pos, ann);
    }

    private static void checkIncreasing(long sId, long dId, long prevSId, long prevDId) {
        if (sId < prevSId) {
            throw new IllegalStateException("Current s_id (" + sId + ") < previous s_id (" + prevSId + ")");
        }
        if (prevSId == sId && dId <= prevDId) {
            throw new IllegalStateException("Current d_id (" + dId + ") <= previous d_id (" + prevDId + ")");
        }
    }

}
