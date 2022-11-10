package aivanov.edge;

import java.nio.ByteBuffer;
import java.util.Objects;

public class Edge {

    static final int sizeInBytes = 8 + 8 + 1 + 8 + 8 + 1;

    public static Edge fromBytes(ByteBuffer byteBuffer) {
        var sId = byteBuffer.getLong();
        var dId = byteBuffer.getLong();
        var st = byteBuffer.get();
        var pos = byteBuffer.getLong();
        var ut = byteBuffer.getLong();
        var ann = byteBuffer.get();
        return new Edge(sId, dId, st, ut, pos, ann);
    }

    public final long sId;
    public final long dId;
    public final byte st;
    public final long ut;
    public final long pos;
    public final byte ann;

    Edge(long sId, long dId, byte st, long ut, long pos, byte ann) {
        this.sId = sId;
        if (sId <= 0) {
            throw new IllegalArgumentException("Non positive s_id: " + sId);
        }

        this.dId = dId;
        if (dId <= 0) {
            throw new IllegalArgumentException("Non positive d_id: " + dId);
        }

        this.st = st;
        if (st < 0 || st > 3) {
            throw new IllegalArgumentException("Unknown state: " + st);
        }

        this.ut = ut;
        if (ut <= 0) {
            throw new IllegalArgumentException("Non positive ut: " + ut);
        }

        this.pos = pos;

        this.ann = ann;
        if (ann < 0) {
            throw new IllegalArgumentException("Negative ann: " + ann);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return sId == edge.sId && dId == edge.dId && st == edge.st && ut == edge.ut && pos == edge.pos && ann == edge.ann;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sId, dId, st, ut, pos, ann);
    }

    @Override
    public String toString() {
        return "Edge{" +
                "sId=" + sId +
                ", dId=" + dId +
                ", st=" + st +
                ", ut=" + ut +
                ", pos=" + pos +
                ", ann=" + ann +
                '}';
    }
}
