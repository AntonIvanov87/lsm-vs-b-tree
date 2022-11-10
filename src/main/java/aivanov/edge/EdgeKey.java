package aivanov.edge;

import com.google.common.hash.Funnel;

public class EdgeKey {

    private final long sId;
    private final long dId;

    public EdgeKey(long sId, long dId) {
        this.sId = sId;
        this.dId = dId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeKey edgeKey = (EdgeKey) o;
        return sId == edgeKey.sId && dId == edgeKey.dId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(31 * sId + dId);
    }

    public static final Funnel<EdgeKey> funnel = (edgeKey, sink) -> sink.putLong(edgeKey.sId).putLong(edgeKey.dId);
}
