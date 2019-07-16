package hdfs.raf.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString
public class WalEntry implements Writable {

  long crc;
  int blockId;
  byte[] data;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(crc);
    out.writeInt(blockId);
    out.writeInt(data.length);
    out.write(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    crc = in.readLong();
    blockId = in.readInt();
    int len = in.readInt();
    if (data == null || len != data.length) {
      data = new byte[len];
    }
    in.readFully(data);
  }

}
