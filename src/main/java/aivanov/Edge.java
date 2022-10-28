package aivanov;

public class Edge {

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

  public static Edge fromCSV(String line) {
    var cells = line.split(",");
    var sId = Long.parseLong(cells[0]);
    var dId = Long.parseLong(cells[1]);
    var st = Byte.parseByte(cells[2]);
    var pos = Long.parseLong(cells[3]);
    var ut = Long.parseLong(cells[4]);
    var ann = Byte.parseByte(cells[5]);
    return new Edge(sId, dId, st, ut, pos, ann);
  }

}
