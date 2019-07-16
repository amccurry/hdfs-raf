package hdfs.raf;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HdfsRandomAccessFile implements ReadWrite {

  private final Configuration _configuration;
  private final Path _dir;
  private final String _name;
  private final File _localCacheDir;
  private final int _length;
  private final BitSet _localCacheBitSet;
  private final int _blockSize;

  public HdfsRandomAccessFile(Configuration configuration, File localCacheDir, Path dir, String name, int length,
      int blockSize) {
    _configuration = configuration;
    _localCacheDir = localCacheDir;
    _dir = dir;
    _name = name;
    _length = length;
    _localCacheBitSet = new BitSet(_length);
    _blockSize = blockSize;
  }

  @Override
  public void writeFully(int position, byte[] buffer, int offset, int length) throws IOException {

  }

  @Override
  public void readFully(int position, byte[] buffer, int offset, int length) throws IOException {


  }

  @Override
  public void close() throws IOException {

  }

}
