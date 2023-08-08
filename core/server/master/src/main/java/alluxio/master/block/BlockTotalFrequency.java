package alluxio.master.block;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.resource.LockResource;
import alluxio.worker.block.BlockFrequencyCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockTotalFrequency {
    @GuardedBy("mFrequencyLock")
    private final Map<Long, Long> mBlockFrequencyMap;
    private final Lock mFrequencyLock ;

    @GuardedBy("mFrequencyLock")
    private long mTotalFrequency;
    private static final Logger LOG = LoggerFactory.getLogger(BlockTotalFrequency.class);

    public BlockTotalFrequency() {
        mBlockFrequencyMap = new ConcurrentHashMap<>();
        mTotalFrequency = 0;
        mFrequencyLock = new ReentrantLock();
    }
    LockResource lockFrequencyMapBlock(){
        return  new LockResource(mFrequencyLock);
    }
    public void sumBlockFrequencyMap(Map<Long,Long> blockFrequencyMap) {
        int fairnessWindowSize = Configuration.getInt(PropertyKey.MASTER_BLOCK_META_FAIRNESS_WINDOW_SIZE);
        try (LockResource r = lockFrequencyMapBlock()){
            for (Map.Entry<Long, Long> entry : blockFrequencyMap.entrySet()) {
                long blockId = entry.getKey();
                long frequency = entry.getValue();
                mTotalFrequency += frequency;
                long totalFrequency = mBlockFrequencyMap.getOrDefault(blockId, 0L);
                mBlockFrequencyMap.put(blockId, totalFrequency + frequency);
                // Keep the map size limited to the most recent 100 block accesses
                if (mTotalFrequency >= fairnessWindowSize) {
                    // You can choose to remove the least recently accessed block or based on some other criteria
                    // For simplicity, we remove the first entry here (not necessarily the least recently accessed)
//        mBlockFrequency.remove(frequencyMap.keySet().iterator().next())
                    calculateFairnessIndex();
                    mBlockFrequencyMap.clear();
                    mTotalFrequency = 0;
                }
            }
        }

        LOG.info("total Frequency: {}",mTotalFrequency);
        LOG.info("block frequency Map: {}",mBlockFrequencyMap);
    }


    private  double calculateMean(Map<Long, Long> blockFrequencyMap) {
        double mean = 0;
        for (Map.Entry<Long, Long> entry : blockFrequencyMap.entrySet()) {
            mean += entry.getValue();
        }
        mean /= blockFrequencyMap.size();
        return mean;
    }

    private  double calculateStandardDeviation(Map<Long, Long> blockFrequencyMap) {
        double mean = calculateMean(blockFrequencyMap);
        double standardDeviation = 0;
        for (Map.Entry<Long, Long> entry : blockFrequencyMap.entrySet()) {
            standardDeviation += Math.pow(entry.getValue() - mean, 2);
        }
        standardDeviation /= blockFrequencyMap.size();
        standardDeviation = Math.sqrt(standardDeviation);
        return standardDeviation;
    }
    private void calculateFairnessIndex(){
        double fairnessThreshold = Configuration.getDouble(PropertyKey.MASTER_BLOCK_META_FAIRNESS_THRESHOLD);
        long sumOfValues = 0;
        long sumOfSquaredValues = 0;

        for (Map.Entry<Long, Long> entry : mBlockFrequencyMap.entrySet()) {
            long value = entry.getValue();
            sumOfValues += value;
            sumOfSquaredValues += value * value;
        }
        long sumSquared = sumOfValues * sumOfValues;
        double fairnessIndex = ((double) sumSquared) / (sumOfSquaredValues * mBlockFrequencyMap.size());

        PropertyKey key = PropertyKey.WORKER_BLOCK_ANNOTATOR_DYNAMIC_SORT;
        String policy = Configuration.getString(key);

        if (fairnessIndex > fairnessThreshold && !policy.equals("REPLICA")){
            LOG.info("fairnessIndex :{}, AI ,change policy {} to REPLICA",fairnessIndex,policy);

            if (Configuration.getBoolean(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED)
                    && key.isDynamic()) {
                Configuration.set(key,"REPLICA", Source.RUNTIME);
            }
        } else if (fairnessIndex <= fairnessThreshold && !policy.equals("LRU") ) {
            LOG.info("fairnessIndex :{}, DA ,change policy {} to LRU",fairnessIndex,policy);

            if (Configuration.getBoolean(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED)
                    && key.isDynamic()) {
                Configuration.set(key,"LRU",Source.RUNTIME);
            }
        }else {
            LOG.info("fairnessIndex :{}, no change policy {}",fairnessIndex,policy);
        }

    }







}
