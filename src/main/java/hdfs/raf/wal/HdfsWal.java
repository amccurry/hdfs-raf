package hdfs.raf.wal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hdfs.raf.util.CRC64;
import hdfs.raf.util.HdfsRafUtil;

public class HdfsWal implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsWal.class);

  private static final Text BLOCK_SIZE_TEXT = new Text("blockSize");
  private static final Text BLOCK_COUNT_TEXT = new Text("blockCount");

  private final Configuration _configuration;
  private final BitSet _localCacheBitSet;
  private final int _blockSize;
  private final int _blockCount;
  private final File _localCacheFile;
  private final RandomAccessFile _rand;
  private final FileChannel _channel;
  private final CRC64 _crc64;
  private final BlockingQueue<WalEntry> _entries;
  private final AtomicBoolean _isClosed = new AtomicBoolean();
  private final AtomicBoolean _error = new AtomicBoolean();
  private final Thread _writerThread;
  private final Path _walPath;
  private final AtomicReference<SequenceFile.Writer> _writer = new AtomicReference<>();
  private final AtomicLong _lastSync = new AtomicLong(System.nanoTime());
  private final AtomicLong _lastWrite = new AtomicLong(System.nanoTime());
  private final boolean _writable;
  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock(true);
  private final WriteLock _writeLock = _lock.writeLock();
  private final ReadLock _readLock = _lock.readLock();
  private final AtomicLong _walLength = new AtomicLong();
  private final Object _walLock = new Object();

  private final long _syncWaitMs = 100;
  private final long _maxSyncWaitTimeNs = TimeUnit.SECONDS.toNanos(5);
  private final long _maxWriteIdleTimeNs = TimeUnit.SECONDS.toNanos(30);
  private final long _hdfsWriteTimeout = TimeUnit.SECONDS.toMillis(1);

  public static HdfsWal create(Configuration configuration, Path walPath, File localCacheFile, int queueCapacity,
      int blockCount, int blockSize) throws IOException {
    return new HdfsWal(configuration, walPath, localCacheFile, queueCapacity, blockCount, blockSize);
  }

  public static HdfsWal recoverForReading(Configuration configuration, Path walPath, File localCacheFile,
      int queueCapacity) throws IOException, InterruptedException {
    return new HdfsWal(configuration, walPath, localCacheFile, queueCapacity);
  }

  /**
   * Recovery constructor.
   * 
   * @param configuration
   * @param walPath
   * @param localCacheFile
   * @param queueCapacity
   * @throws IOException
   */
  private HdfsWal(Configuration configuration, Path walPath, File localCacheFile, int queueCapacity)
      throws IOException {
    _configuration = configuration;
    _writable = false;
    _walPath = walPath;
    _entries = null;
    _crc64 = CRC64.newInstance();
    _localCacheFile = localCacheFile;
    try (SequenceFile.Reader reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(walPath))) {
      Metadata metadata = reader.getMetadata();
      Text blockCountText = metadata.get(BLOCK_COUNT_TEXT);
      Text blockSizeText = metadata.get(BLOCK_SIZE_TEXT);
      _blockCount = Integer.parseInt(blockCountText.toString());
      _blockSize = Integer.parseInt(blockSizeText.toString());
      _localCacheFile.delete();
      _localCacheBitSet = new BitSet(_blockCount);

      HdfsRafUtil.createLocalSparseFile(_localCacheFile, _blockCount * _blockSize);
      try (RandomAccessFile rand = new RandomAccessFile(localCacheFile, "rw")) {
        try (FileChannel channel = rand.getChannel()) {
          WalEntry walEntry = new WalEntry();
          while (reader.next(walEntry)) {
            recoverWalEntry(walEntry, channel);
          }
        }
      }
    }
    _rand = new RandomAccessFile(_localCacheFile, "r");
    _channel = _rand.getChannel();
    _writerThread = null;
  }

  /**
   * Writable constructor.
   * 
   * @param configuration
   * @param walPath
   * @param localCacheFile
   * @param queueCapacity
   * @param blockCount
   * @param blockSize
   * @throws IOException
   */
  private HdfsWal(Configuration configuration, Path walPath, File localCacheFile, int queueCapacity, int blockCount,
      int blockSize) throws IOException {
    _writable = true;
    _configuration = configuration;
    _localCacheBitSet = new BitSet(blockCount);
    _blockCount = blockCount;
    _blockSize = blockSize;
    _walPath = walPath;
    _localCacheFile = localCacheFile;
    _entries = new ArrayBlockingQueue<>(queueCapacity);
    HdfsRafUtil.createLocalSparseFile(_localCacheFile, blockCount * blockSize);
    _rand = new RandomAccessFile(_localCacheFile, "rw");
    _channel = _rand.getChannel();
    _crc64 = CRC64.newInstance();
    _writerThread = new Thread(() -> {
      try {
        writeEntries();
      } catch (Exception e) {
        _error.set(true);
        LOGGER.error("Unknown error", e);
      }
    });
    _writerThread.setName(HdfsWal.class.getName() + " " + _walPath);
    _writerThread.start();
  }

  public boolean isWriterIdle() {
    return _writer.get() == null;
  }

  public void removeWalFile() throws IOException {
    FileSystem fileSystem = _walPath.getFileSystem(_configuration);
    fileSystem.delete(_walPath, false);
  }

  public int getBlockSize() {
    return _blockSize;
  }

  public int getBlockCount() {
    return _blockCount;
  }

  public long getLength() {
    return _walLength.get();
  }

  public boolean hasErrorOccurred() {
    return _error.get();
  }

  /**
   * Write the block into the wal, cache locally and return the current CRC.
   * 
   * @param blockId
   *          the block id.
   * @param buffer
   *          the buffer.
   * @param offset
   *          the offset in the buffer.
   * @param length
   *          the length of the buffer to write.
   * @return the current crc.
   * @throws IOException
   * @throws InterruptedException
   */
  public long writeBlock(int blockId, byte[] buffer, int offset, int length) throws IOException, InterruptedException {
    writeCheck();
    closedCheck();
    if (length != _blockSize) {
      throw new IOException("Length " + length + " does not equal blocksize " + _blockSize);
    }
    return internalWriteBlock(false, blockId, buffer, offset, length, _channel);
  }

  /**
   * Reads the block into the buffer and returns true if this wal cache had the
   * data locally or false if the data was missing.
   * 
   * @param blockId
   *          the block id.
   * @param buffer
   *          the buffer.
   * @param offset
   *          the offset into the buffer.
   * @param length
   *          the length to read.
   * @return true if read was successful and false if not.
   * @throws IOException
   */
  public boolean readBlock(int blockId, byte[] buffer, int offset, int length) throws IOException {
    _readLock.lock();
    closedCheck();
    try {
      if (_localCacheBitSet.get(blockId)) {
        long position = getPosition(blockId);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
        while (byteBuffer.hasRemaining()) {
          position += _channel.read(byteBuffer, position);
        }
        return true;
      }
      return false;
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    _writeLock.lock();
    try {
      _isClosed.set(true);
    } finally {
      _writeLock.unlock();
    }
    IOUtils.closeQuietly(_channel);
    IOUtils.closeQuietly(_rand);
    _localCacheFile.delete();
    stopWritingThread();
    if (_writable) {
      if (!_entries.isEmpty()) {
        throw new IOException("All wal entries not written.");
      }
      closeWriter();
    }
  }

  private void recoverWalEntry(WalEntry walEntry, FileChannel channel) throws IOException {
    long crc = walEntry.getCrc();
    int blockId = walEntry.getBlockId();
    byte[] data = walEntry.getData();
    try {
      long resultCrc = internalWriteBlock(true, blockId, data, 0, _blockSize, channel);
      if (crc != resultCrc) {
        throw new IOException(
            "Crc " + crc + " from wal entry does not match result crc " + resultCrc + " on recovery.");
      }
    } catch (InterruptedException e) {
      // should not occur because we are skipping the wal write on
      // recovery
      throw new IOException(e);
    }
  }

  private long internalWriteBlock(boolean skipWal, int blockId, byte[] buffer, int offset, int length,
      FileChannel channel) throws IOException, InterruptedException {
    _writeLock.lock();
    try {
      _crc64.update(blockId);
      long crc = _crc64.update(buffer, offset, length);

      if (!skipWal) {
        recordToWal(crc, blockId, buffer, offset, length);
      }

      long position = getPosition(blockId);

      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
      while (byteBuffer.hasRemaining()) {
        position += channel.write(byteBuffer, position);
      }
      _localCacheBitSet.set(blockId);
      return crc;
    } finally {
      _writeLock.unlock();
    }
  }

  private void stopWritingThread() {
    if (_writable) {
      try {
        _writerThread.join(TimeUnit.SECONDS.toMillis(120));
      } catch (InterruptedException e) {
        LOGGER.error("Unknown error", e);
      }
      if (_writerThread.isAlive()) {
        LOGGER.error("Hdfs writer did not exit, interrupting");
        _writerThread.interrupt();
      }
    }
  }

  private void writeCheck() throws IOException {
    if (!_writable) {
      throw new IOException("Read only");
    }
  }

  private void recordToWal(long crc, int blockId, byte[] buffer, int offset, int length) throws InterruptedException {
    WalEntry walEntry = new WalEntry();
    walEntry.setCrc(crc);
    walEntry.setBlockId(blockId);
    walEntry.setData(copy(buffer, offset, length));
    _entries.put(walEntry);
  }

  private byte[] copy(byte[] buffer, int offset, int length) {
    byte[] buf = new byte[length];
    System.arraycopy(buffer, offset, buf, 0, length);
    return buf;
  }

  private long getPosition(int blockId) {
    return blockId * _blockSize;
  }

  private void closedCheck() throws IOException {
    if (_isClosed.get()) {
      throw new IOException("HdfsWal " + _walPath + " already closed");
    }
  }

  private void writeEntries() throws Exception {
    List<WalEntry> entriesToWrite = new ArrayList<>();
    try {
      while (!_isClosed.get()) {
        entriesToWrite.clear();
        synchronized (_walLock) {
          WalEntry walEntry = _entries.poll(_hdfsWriteTimeout, TimeUnit.MILLISECONDS);
          if (walEntry == null) {
            closeWriterIfNeeded();
          } else {
            SequenceFile.Writer writer = getWriter();
            writer.append(walEntry, NullWritable.get());
            _entries.drainTo(entriesToWrite);
            for (WalEntry entry : entriesToWrite) {
              writer.append(entry, NullWritable.get());
            }
            _lastWrite.set(System.nanoTime());
            _walLength.set(writer.getLength());
            syncWriteIfNeeded(writer);
          }
        }
      }
    } finally {
      drainEntries(entriesToWrite);
    }
  }

  public void sync() throws IOException {
    _writeLock.lock();
    try {
      waitForPendingWrites();
      Writer writer = _writer.get();
      if (writer != null) {
        writer.hflush();
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private void waitForPendingWrites() throws IOException {
    while (hasPendingWrites()) {
      try {
        Thread.sleep(_syncWaitMs);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  private boolean hasPendingWrites() {
    return !_entries.isEmpty();
  }

  private void drainEntries(List<WalEntry> entriesToWrite) throws IOException {
    if (_entries.isEmpty()) {
      return;
    }
    while (!_entries.isEmpty()) {
      try (SequenceFile.Writer writer = getWriter()) {
        _writer.set(null);
        _entries.drainTo(entriesToWrite);
        for (WalEntry entry : entriesToWrite) {
          writer.append(entry, NullWritable.get());
        }
      }
    }
  }

  private void closeWriterIfNeeded() {
    if (_lastWrite.get() + _maxWriteIdleTimeNs < System.nanoTime()) {
      closeWriter();
    }
  }

  private void syncWriteIfNeeded(Writer writer) throws IOException {
    if (_lastSync.get() + _maxSyncWaitTimeNs < System.nanoTime()) {
      writer.hflush();
      _lastSync.set(System.nanoTime());
    }
  }

  private void closeWriter() {
    IOUtils.closeQuietly(_writer.getAndSet(null));
  }

  private Writer getWriter() throws IOException {
    Writer writer = _writer.get();
    if (writer == null) {
      Metadata metadata = new Metadata();
      // metadata.set(name, value);

      metadata.set(BLOCK_SIZE_TEXT, new Text(Integer.toString(_blockSize)));
      metadata.set(BLOCK_COUNT_TEXT, new Text(Integer.toString(_blockCount)));

      _writer.set(writer = SequenceFile.createWriter(_configuration, SequenceFile.Writer.appendIfExists(true),
          SequenceFile.Writer.file(_walPath), SequenceFile.Writer.keyClass(WalEntry.class),
          SequenceFile.Writer.valueClass(NullWritable.class), SequenceFile.Writer.metadata(metadata)));

    }
    return writer;
  }

}
