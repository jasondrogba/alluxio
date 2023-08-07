package alluxio.master.block;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BlockTotalFrequency {
    private static final Map<Long, Long> mBlockFrequencyMap
            = new ConcurrentHashMap<>();

    public BlockTotalFrequency() {

    }

    public static void sumBlockFrequencyMap(Map<Long,Long> blockFrequencyMap) {
        for (Map.Entry<Long, Long> entry : blockFrequencyMap.entrySet()) {
            long blockId = entry.getKey();
            long frequency = entry.getValue();
            long totalFrequency = mBlockFrequencyMap.getOrDefault(blockId, 0L);
            mBlockFrequencyMap.put(blockId, totalFrequency + frequency);

            // Keep the map size limited to the most recent 100 block accesses
            if (mBlockFrequencyMap.size() > 100) {
                // You can choose to remove the least recently accessed block or based on some other criteria
                // For simplicity, we remove the first entry here (not necessarily the least recently accessed)
                mBlockFrequencyMap.remove(mBlockFrequencyMap.keySet().iterator().next());
            }
        }
    }

    public static Map<Long, Long> getBlockFrequencyMap() {
        return mBlockFrequencyMap;
    }

    public static void clearBlockFrequencyMap() {
        mBlockFrequencyMap.clear();
    }

    //calculate the total frequency of all blocks

    private static double calculateMean(Map<Long, Long> blockFrequencyMap) {
        double mean = 0;
        for (Map.Entry<Long, Long> entry : blockFrequencyMap.entrySet()) {
            mean += entry.getValue();
        }
        mean /= blockFrequencyMap.size();
        return mean;
    }

    private static double calculateStandardDeviation(Map<Long, Long> blockFrequencyMap) {
        double mean = calculateMean(blockFrequencyMap);
        double standardDeviation = 0;
        for (Map.Entry<Long, Long> entry : blockFrequencyMap.entrySet()) {
            standardDeviation += Math.pow(entry.getValue() - mean, 2);
        }
        standardDeviation /= blockFrequencyMap.size();
        standardDeviation = Math.sqrt(standardDeviation);
        return standardDeviation;
    }

    private  static double calculateFairness(Map<Long, Long> blockFrequencyMap) {
        double fairness = 0;
        long sumPower=0;
        long powerSum=0;

        for (Map.Entry<Long, Long> entry : blockFrequencyMap.entrySet()) {
            sumPower += (long) Math.pow(entry.getValue(),2);
            powerSum += entry.getValue();
        }
        powerSum = (long)Math.pow(powerSum,2);
        fairness = (double) powerSum / (double) blockFrequencyMap.size() / (double)sumPower;
        return fairness;
    }








}
