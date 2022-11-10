package aivanov.edge;

import aivanov.ProgressPrinter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

class CheckBinaryFile {

    public static void main(String[] args) throws IOException {
        var progressPrinter = new ProgressPrinter(Edges.numEdgesInFile);
        try (var binFileIterator = new BinFileIterator()) {
            // BinFileIterator checks checksum in the end
            while (binFileIterator.hasNext()) {
                binFileIterator.next();
                progressPrinter.incProgress();
            }
        }

        if (progressPrinter.processed() != Edges.numEdgesInFile) {
            throw new IllegalStateException("Processed " + progressPrinter.processed() + " edges != expected " + Edges.numEdgesInFile);
        }

        var sec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - progressPrinter.startNanos);
        var totalPerSec = progressPrinter.processed() / sec;
        System.out.println("Checked " + progressPrinter.processed() + " edges in " + sec + " sec, average speed " + totalPerSec + " / sec");
    }

}
