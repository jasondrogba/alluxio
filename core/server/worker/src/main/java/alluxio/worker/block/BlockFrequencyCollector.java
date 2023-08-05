package alluxio.worker.block;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BlockFrequencyCollector {
    private static final Map<Long, Long> mblockFrequencyMap
            = new ConcurrentHashMap<>();

    public BlockFrequencyCollector() {
    }

    public void collectBlockAccess(long blockId) {
        long frequency = mblockFrequencyMap.getOrDefault(blockId, 0L);
        mblockFrequencyMap.put(blockId, frequency + 1);

        // Keep the map size limited to the most recent 100 block accesses
        if (mblockFrequencyMap.size() > 100) {
            // You can choose to remove the least recently accessed block or based on some other criteria
            // For simplicity, we remove the first entry here (not necessarily the least recently accessed)
            mblockFrequencyMap.remove(mblockFrequencyMap.keySet().iterator().next());
        }
    }

    public long getBlockAccessFrequency(long blockId) {
        return mblockFrequencyMap.getOrDefault(blockId, 0L);
    }

    public static Map<Long, Long> getBlockFrequencyMap() {
        return mblockFrequencyMap;
    }

}
