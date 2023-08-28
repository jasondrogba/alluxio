/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.PositionReader;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;

import com.amazonaws.annotation.NotThreadSafe;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Implementation of {@link FileInStream} that reads from a dora cache if possible.
 */
@NotThreadSafe
public class PositionReadFileInStream extends FileInStream {
  private final long mLength;
  private long mPos = 0;
  private boolean mClosed;
  private final PositionReader mPositionReader;
  private final PrefetchCache mCache;

  /**
   * Constructor.
   * @param reader
   * @param length
   */
  public PositionReadFileInStream(PositionReader reader,
      long length) {
    mPositionReader = reader;
    mLength = length;
    mCache = new PrefetchCache(
        Configuration.getInt(PropertyKey.USER_POSITION_READER_STREAMING_MULTIPLIER), mLength);
  }

  @Override
  public long remaining() {
    return mLength - mPos;
  }

  @VisibleForTesting
  int getBufferedLength() {
    return mCache.getCache().readableBytes();
  }

  @VisibleForTesting
  long getBufferedPosition() {
    return mCache.getCacheStartPos();
  }

  @VisibleForTesting
  int getPrefetchSize() {
    return mCache.getPrefetchSize();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "Read buffer cannot be null");
    return read(ByteBuffer.wrap(b), off, len);
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    byteBuffer.position(off).limit(off + len);
    mCache.addTrace(mPos, len);
    int totalBytesRead = 0;
    int bytesReadFromCache = mCache.fillWithCache(mPos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    mPos += bytesReadFromCache;
    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }
    int bytesPrefetched = mCache.prefetch(mPositionReader, mPos, byteBuffer.remaining());
    if (bytesPrefetched < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }
    bytesReadFromCache = mCache.fillWithCache(mPos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    mPos += bytesReadFromCache;
    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }
    int bytesRead = mPositionReader.read(mPos, byteBuffer, byteBuffer.remaining());
    if (bytesRead < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }
    totalBytesRead += bytesRead;
    mPos += bytesRead;
    return totalBytesRead;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int len)
      throws IOException {
    long pos = position;
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, len);
    mCache.addTrace(position, len);
    int totalBytesRead = 0;
    int bytesReadFromCache = mCache.fillWithCache(pos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    pos += bytesReadFromCache;
    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }
    int bytesPrefetched = mCache.prefetch(mPositionReader, pos, byteBuffer.remaining());
    if (bytesPrefetched < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }
    bytesReadFromCache = mCache.fillWithCache(pos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    pos += bytesReadFromCache;
    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }
    int bytesRead = mPositionReader.read(pos, byteBuffer, byteBuffer.remaining());
    if (bytesRead < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }
    totalBytesRead += bytesRead;
    pos += bytesRead;
    return totalBytesRead;
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        "Seek position past the end of the read region (block or file).");
    if (pos == mPos) {
      return;
    }
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    seek(mPos + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mPositionReader.close();
    mCache.close();
  }
}


