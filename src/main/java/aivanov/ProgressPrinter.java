package aivanov;

import java.util.concurrent.TimeUnit;

public class ProgressPrinter {

    public final long startNanos = System.nanoTime();
    private final long total;
    private long processed = 0;
    private long prevProcessed = 0;
    private long prevPrintNanos = System.nanoTime();

    public ProgressPrinter(long total) {
        this.total = total;
    }

    public void incProgress() {
        processed++;

        var nanoTime = System.nanoTime();
        if (TimeUnit.NANOSECONDS.toSeconds(nanoTime - prevPrintNanos) >= 10) {
            printProgress();
            prevProcessed = processed;
            prevPrintNanos = nanoTime;
        }
    }

    public long processed() {
        return processed;
    }

    private void printProgress() {
        var percent = 100 * processed / total;
        var avgSpeedPerSec = processed / TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
        var curSpeedPerSec = (processed - prevProcessed) / 10;
        System.out.println("Processed " + processed + " (" + percent + "%) , average speed " + avgSpeedPerSec + " / sec, current speed " + curSpeedPerSec + " / sec");
    }

}
