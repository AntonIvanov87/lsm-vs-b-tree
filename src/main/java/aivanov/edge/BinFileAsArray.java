package aivanov.edge;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class BinFileAsArray implements AutoCloseable {

    private final FileChannel fileChannel = FileChannel.open(Edges.binFile, StandardOpenOption.READ);
    public final long length = fileChannel.size() / Edge.sizeInBytes;
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Edge.sizeInBytes);

    public BinFileAsArray() throws IOException {
    }

    public Edge get(long index) throws IOException {
        fileChannel.position(index * Edge.sizeInBytes);
        fileChannel.read(byteBuffer);
        byteBuffer.flip();
        return Edge.fromBytes(byteBuffer);
    }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }
}
