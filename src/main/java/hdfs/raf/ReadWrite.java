package hdfs.raf;

import java.io.Closeable;
import java.io.IOException;

public interface ReadWrite extends Closeable {

  void writeFully(int position, byte[] buffer, int offset, int length) throws IOException;

  void readFully(int position, byte[] buffer, int offset, int length) throws IOException;

}
