package alluxio.client.file;
import com.google.common.collect.EvictingQueue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import alluxio.PositionReader;
import alluxio.network.protocol.databuffer.PooledDirectNioByteBuf;
import java.io.IOException;


public class PrefetchCache implements AutoCloseable {
  private final long mFileLength;
  private final EvictingQueue<CallTrace> mCallHistory;
  private int mPrefetchSize = 0;

  private ByteBuf mCache = Unpooled.wrappedBuffer(new byte[0]);
  private long mCacheStartPos = 0;

  PrefetchCache(int prefetchMultiplier, long fileLength) {
    mCallHistory = EvictingQueue.create(prefetchMultiplier);
    mFileLength = fileLength;
  }

  private void update() {
    int consecutiveReadLength = 0;
    long lastReadEnd = -1;
    for (CallTrace trace : mCallHistory) {
      if (trace.getPosition() == lastReadEnd) {
        lastReadEnd += trace.getLength();
        consecutiveReadLength += trace.getLength();
      } else {
        lastReadEnd = trace.getPosition() + trace.getLength();
        consecutiveReadLength = trace.getLength();
      }
    }
    mPrefetchSize = consecutiveReadLength;
  }

  public void addTrace(long pos, int size) {
    mCallHistory.add(new CallTrace(pos, size));
    update();
  }

  /**
   * Fills the output with bytes from the prefetch cache.
   *
   * @param targetStartPos the position within the file to read from
   * @param outBuffer output buffer
   * @return number of bytes copied from the cache, 0 if the cache does not contain the requested
   *         range of data
   */
  public int fillWithCache(long targetStartPos, ByteBuffer outBuffer) {
    if (mCacheStartPos <= targetStartPos) {
      if (targetStartPos - mCacheStartPos < mCache.readableBytes()) {
        final int posInCache = (int) (targetStartPos - mCacheStartPos);
        final int size = Math.min(outBuffer.remaining(), mCache.readableBytes() - posInCache);
        ByteBuffer slice = outBuffer.slice();
        slice.limit(size);
        mCache.getBytes(posInCache, slice);
        outBuffer.position(outBuffer.position() + size);
        return size;
      } else {
        // the position is beyond the cache end position
        return 0;
      }
    } else {
      // the position is behind the cache start position
      return 0;
    }
  }

  /**
   * Prefetches and caches data from the reader.
   *
   * @param reader reader
   * @param pos position within the file
   * @param minBytesToRead minimum number of bytes to read from the reader
   * @return number of bytes that's been prefetched, 0 if exception occurs
   */
  public int prefetch(PositionReader reader, long pos, int minBytesToRead) {
    int prefetchSize = Math.max(mPrefetchSize, minBytesToRead);
    // cap to remaining file length
    prefetchSize = (int) Math.min(mFileLength - pos, prefetchSize);

    if (mCache.capacity() < prefetchSize) {
      mCache.release();
      mCache = PooledDirectNioByteBuf.allocate(prefetchSize);
      mCacheStartPos = 0;
    }
    mCache.clear();
    try {
      int bytesPrefetched = reader.read(pos, mCache, prefetchSize);
      if (bytesPrefetched > 0) {
        mCache.readerIndex(0).writerIndex(bytesPrefetched);
        mCacheStartPos = pos;
      }
      return bytesPrefetched;
    } catch (IOException ignored) {
      // silence exceptions as we don't care if prefetch fails
      mCache.clear();
      return 0;
    }
  }

  @Override
  public void close() {
    mCache.release();
    mCache = Unpooled.wrappedBuffer(new byte[0]);
    mCacheStartPos = 0;
  }
  public ByteBuf getCache() {
    return mCache;
  }

  public long getCacheStartPos() {
    return mCacheStartPos;
  }

  public int getPrefetchSize() {
    return mPrefetchSize;
  }
}

