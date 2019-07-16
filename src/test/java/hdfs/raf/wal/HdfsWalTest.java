package hdfs.raf.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import hdfs.raf.util.CRC64;

public class HdfsWalTest {
  private static MiniDFSCluster CLUSTER;
  private static File STORE_PATH = new File("./target/tmp/" + HdfsWalTest.class.getName());
  private static FileSystem FILESYSTEM;

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration configuration = new Configuration();
    String storePath = new File(STORE_PATH, "hdfs").getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    CLUSTER = new MiniDFSCluster.Builder(configuration).build();
    FILESYSTEM = CLUSTER.getFileSystem();
  }

  @AfterClass
  public static void afterClass() {
    CLUSTER.shutdown();
  }

  @Test
  public void testHdfsWalNoOpTest() throws IOException {
    Path walPath = new Path("/wal." + UUID.randomUUID()
                                          .toString());
    File localCacheFile = new File(STORE_PATH, UUID.randomUUID()
                                                   .toString());
    int queueCapacity = 10;
    int blockCount = 1000;
    int blockSize = 1000;
    try (HdfsWal wal = HdfsWal.create(getConf(), walPath, localCacheFile, queueCapacity, blockCount, blockSize)) {
      assertTrue(wal.isWriterIdle());
      assertTrue(localCacheFile.exists());
      wal.removeWalFile();
    }
    assertFalse(localCacheFile.exists());
    assertFalse(FILESYSTEM.exists(walPath));
  }

  @Test
  public void testHdfsWalSimpleTest() throws IOException, InterruptedException {
    Path walPath = new Path("/wal." + UUID.randomUUID()
                                          .toString());
    File localCacheFile = new File(STORE_PATH, UUID.randomUUID()
                                                   .toString());
    int queueCapacity = 10;
    int blockCount = 1000;
    int blockSize = 1000;
    int passes = 1000;

    long seed = getSeed();
    try (HdfsWal wal = HdfsWal.create(getConf(), walPath, localCacheFile, queueCapacity, blockCount, blockSize)) {
      assertTrue(wal.isWriterIdle());
      assertTrue(localCacheFile.exists());
      testWalReadsAndWrites(seed, passes, wal);
      assertFalse(wal.isWriterIdle());
      wal.removeWalFile();
    }
    assertFalse(localCacheFile.exists());
    assertFalse(FILESYSTEM.exists(walPath));
  }

  @Test
  public void testHdfsWalCleanRecoveryTest() throws IOException, InterruptedException {
    Path walPath = new Path("/wal." + UUID.randomUUID()
                                          .toString());
    File localCacheFile = new File(STORE_PATH, UUID.randomUUID()
                                                   .toString());
    int queueCapacity = 10;
    int blockCount = 1000;
    int blockSize = 1000;
    int passes = 1000;

    long seed = getSeed();
    long[] crcs;
    try (HdfsWal wal = HdfsWal.create(getConf(), walPath, localCacheFile, queueCapacity, blockCount, blockSize)) {
      assertTrue(wal.isWriterIdle());
      assertTrue(localCacheFile.exists());
      crcs = testWalReadsAndWrites(seed, passes, wal);
      assertFalse(wal.isWriterIdle());
    }
    assertFalse(localCacheFile.exists());
    try (HdfsWal wal = HdfsWal.recoverForReading(getConf(), walPath, localCacheFile, queueCapacity)) {
      assertTrue(wal.isWriterIdle());
      assertTrue(localCacheFile.exists());
      validCrcs(seed, passes, wal, crcs);
      assertTrue(wal.isWriterIdle());
      wal.removeWalFile();
    }
    assertFalse(localCacheFile.exists());
    assertFalse(FILESYSTEM.exists(walPath));
  }

  @Test
  public void testHdfsWalDirtyRecoveryTest() throws IOException, InterruptedException {
    Path walPath = new Path("/wal." + UUID.randomUUID()
                                          .toString());
    File localCacheFile = new File(STORE_PATH, UUID.randomUUID()
                                                   .toString());
    int queueCapacity = 10;
    int blockCount = 1000;
    int blockSize = 1000;
    int passes = 1000;

    long seed = getSeed();
    long[] crcs;
    {
      HdfsWal wal = HdfsWal.create(getConf(), walPath, localCacheFile, queueCapacity, blockCount, blockSize);
      assertTrue(wal.isWriterIdle());
      assertTrue(localCacheFile.exists());
      crcs = testWalReadsAndWrites(seed, passes, wal);
      wal.sync();
      assertFalse(wal.isWriterIdle());
    }
    assertTrue(localCacheFile.exists());// because clean up didn't work
    localCacheFile.delete();

    try (HdfsWal wal = HdfsWal.recoverForReading(getConf(), walPath, localCacheFile, queueCapacity)) {
      assertTrue(wal.isWriterIdle());
      assertTrue(localCacheFile.exists());
      validCrcs(seed, passes, wal, crcs);
      assertTrue(wal.isWriterIdle());
      wal.removeWalFile();
    }
    assertFalse(localCacheFile.exists());
    assertFalse(FILESYSTEM.exists(walPath));
  }

  private long getSeed() {
    return new Random().nextLong();
  }

  private long[] testWalReadsAndWrites(long seed, int passes, HdfsWal wal) throws IOException, InterruptedException {
    long[] crcs = writeAndValidData(seed, passes, wal);
    validCrcs(seed, passes, wal, crcs);
    return crcs;
  }

  private long[] writeAndValidData(long seed, int passes, HdfsWal wal) throws IOException, InterruptedException {
    int blockCount = wal.getBlockCount();
    int blockSize = wal.getBlockSize();
    byte[] writeBuffer = new byte[blockSize];
    byte[] readBuffer = new byte[blockSize];
    long[] crcs = new long[blockCount];
    Random random = new Random(seed);
    for (int p = 0; p < passes; p++) {
      random.nextBytes(writeBuffer);
      int blockId = random.nextInt(blockCount);
      crcs[blockId] = CRC64.newInstance()
                           .update(writeBuffer);

      wal.writeBlock(blockId, writeBuffer, 0, blockSize);
      assertTrue("seed " + seed, wal.readBlock(blockId, readBuffer, 0, blockSize));
      assertTrue("seed " + seed, Arrays.equals(writeBuffer, readBuffer));
    }
    return crcs;
  }

  private void validCrcs(long seed, int passes, HdfsWal wal, long[] crcs) throws IOException {
    int blockCount = wal.getBlockCount();
    int blockSize = wal.getBlockSize();
    byte[] writeBuffer = new byte[blockSize];
    byte[] readBuffer = new byte[blockSize];
    Random random = new Random(seed);
    for (int p = 0; p < passes; p++) {
      random.nextBytes(writeBuffer);
      int blockId = random.nextInt(blockCount);
      long writeCrc = CRC64.newInstance()
                           .update(writeBuffer);

      if (writeCrc != crcs[blockId]) {
        // This happens when we write over the same block id in a set of
        // passes
        continue;
      }
      assertTrue("seed " + seed, wal.readBlock(blockId, readBuffer, 0, blockSize));
      long readCrc = CRC64.newInstance()
                          .update(readBuffer);
      assertEquals("seed " + seed, writeCrc, readCrc);
    }
  }

  private Configuration getConf() {
    return FILESYSTEM.getConf();
  }
}
