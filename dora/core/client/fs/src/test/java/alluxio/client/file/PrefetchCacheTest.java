package alluxio.client.file;


import alluxio.PositionReader;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import java.io.IOException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

public class PrefetchCacheTest {
    @Mock
    private PositionReader mockPositionReader;

    private PrefetchCache prefetchCache;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        prefetchCache = new PrefetchCache(16, 100);
    }

    @Test
    public void testAddTraceAndUpdate() {
        prefetchCache.addTrace(0, 10);
        prefetchCache.addTrace(10, 20);
        assertEquals(30, prefetchCache.getPrefetchSize());

    }

    @Test
    public void testFillWithCache() {
        ByteBuffer mockCache = Mockito.mock(ByteBuffer.class);
        prefetchCache.fillWithCache(10, mockCache);
        assertEquals(10, prefetchCache.getPrefetchSize());
        // Add more test cases as needed
    }

    @Test
    public void testPrefetch() throws IOException {

        when(mockPositionReader.read(anyLong(), any(ByteBuffer.class), anyInt())).thenReturn(15);
        int bytesRead = prefetchCache.prefetch(mockPositionReader, 0, 15);
        assertEquals(15, bytesRead);
        assertEquals(15, prefetchCache.getPrefetchSize());
    }

    @Test
    public void testPrefetchWithException() throws IOException {
        when(mockPositionReader.read(anyLong(),  any(ByteBuffer.class), anyInt())).thenThrow(new IOException());
        int bytesRead = prefetchCache.prefetch(mockPositionReader, 0, 15);
        assertEquals(0, bytesRead);
        assertEquals(0, prefetchCache.getPrefetchSize());
    }

    // Add more test cases for other methods as needed

    @Test
    public void testClose() {
        prefetchCache.close();
        assertEquals(0, prefetchCache.getPrefetchSize());
        assertEquals(0, prefetchCache.getCache().capacity());
        assertEquals(0, prefetchCache.getCacheStartPos());
    }
}