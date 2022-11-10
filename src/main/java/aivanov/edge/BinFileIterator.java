package aivanov.edge;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class BinFileIterator implements Iterator<Edge>, AutoCloseable {

    private final FileChannel fileChannel = FileChannel.open(Edges.binFile, StandardOpenOption.READ);
    // Aligned to page size
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096 * Edge.sizeInBytes);
    private final Checksum checksum = new Adler32();
    private final ArrayList<Edge> edgesBuffer = new ArrayList<>(4096);
    private Iterator<Edge> edgesBufferIterator = edgesBuffer.iterator();

    public BinFileIterator() throws IOException {
    }

    @Override
    public boolean hasNext() {
        if (edgesBufferIterator.hasNext()) {
            return true;
        }
        if (!fileChannel.isOpen()) {
            return false;
        }
        try {
            fileChannel.read(byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read edges from " + Edges.binFile, e);
        }
        if (byteBuffer.limit() != byteBuffer.capacity()) {
            // The end
            byteBuffer.limit(byteBuffer.limit() - 8);  // The last 8 bytes is a checksum
            try {
                fileChannel.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close " + Edges.binFile, e);
            }
        }

        byteBuffer.flip();
        checksum.update(byteBuffer);
        if (!fileChannel.isOpen()) {
            byteBuffer.limit(byteBuffer.limit() + 8);
            var checksumFromFile = byteBuffer.getLong();
            if (checksum.getValue() != checksumFromFile) {
                throw new IllegalStateException("Checksum of edges != checksum from file");
            }
        }

        edgesBuffer.clear();
        byteBuffer.flip();
        while (byteBuffer.remaining() >= Edge.sizeInBytes) {
            edgesBuffer.add(Edge.fromBytes(byteBuffer));
        }
        byteBuffer.clear();
        edgesBufferIterator = edgesBuffer.iterator();
        return edgesBufferIterator.hasNext();
    }

    @Override
    public Edge next() {
        return edgesBufferIterator.next();
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
