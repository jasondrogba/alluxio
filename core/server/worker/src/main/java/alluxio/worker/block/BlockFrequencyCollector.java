package alluxio.worker.block;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BlockFrequencyCollector {
    private static final Logger LOG = LoggerFactory.getLogger(BlockFrequencyCollector.class);

    private static final Map<Long, Long> mblockFrequencyMap
            = new ConcurrentHashMap<>();

    public BlockFrequencyCollector() {
    }

    public void collectBlockAccess(long blockId) {
        long frequency = mblockFrequencyMap.getOrDefault(blockId, 0L);
        mblockFrequencyMap.put(blockId, frequency + 1);
        LOG.info("blockId : {}, frequency :{}",blockId,mblockFrequencyMap.get(blockId));

        // Keep the map size limited to the most recent 100 block accesses
        if (mblockFrequencyMap.size() > 100) {
            // You can choose to remove the least recently accessed block or based on some other criteria
            // For simplicity, we remove the first entry here (not necessarily the least recently accessed)
            mblockFrequencyMap.remove(mblockFrequencyMap.keySet().iterator().next());
        }
    }

    public static Map<Long, Long> getBlockFrequencyMap() {
        Map<Long, Long> frequencyResult = new ConcurrentHashMap<>(mblockFrequencyMap);
        mblockFrequencyMap.clear();
        if (!frequencyResult.isEmpty()){
            LOG.info("clear block FrequencyMap:{},frequency Result:{}",mblockFrequencyMap,frequencyResult);
        }
        return frequencyResult;
    }

}
