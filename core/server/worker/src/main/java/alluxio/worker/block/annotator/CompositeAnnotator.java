package alluxio.worker.block.annotator;

import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Reconfigurable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import  alluxio.worker.block.annotator.LRFUAnnotator;
import alluxio.worker.block.annotator.ReplicaBasedAnnotator;
public class CompositeAnnotator implements BlockAnnotator<CompositeAnnotator.CompositeSortedField>, Reconfigurable {
    private static final Logger LOG = LoggerFactory.getLogger(CompositeAnnotator.class);

    /** In the range of [0, 1]. Closer to 0, Composite closer to LRFU. Closer to 1, Composite closer to ReplicaLRU. */
    private static  double COMPOSITE_RATIO;
    /** In the range of [0, 1]. Closer to 0, LRFU closer to LFU. Closer to 1, LRFU closer to LRU. */
    private static final double STEP_FACTOR;
    /** The attenuation factor is in the range of [2, INF]. */
    private static final double ATTENUATION_FACTOR;
    private static final double LRU_RATIO;
    private static final double REPLICA_RATIO;
    private final AtomicLong mLRUClock ;
    static {
        COMPOSITE_RATIO = Configuration.getDouble(
                PropertyKey.WORKER_BLOCK_ANNOTATOR_COMPOSITE_RATIO);
        STEP_FACTOR = Configuration.getDouble(
                PropertyKey.WORKER_BLOCK_ANNOTATOR_LRFU_STEP_FACTOR);
        ATTENUATION_FACTOR = Configuration.getDouble(
                PropertyKey.WORKER_BLOCK_ANNOTATOR_LRFU_ATTENUATION_FACTOR);
        LRU_RATIO = Configuration.getDouble(
                PropertyKey.WORKER_BLOCK_ANNOTATOR_REPLICA_LRU_RATIO);
        REPLICA_RATIO = Configuration.getDouble(
                PropertyKey.WORKER_BLOCK_ANNOTATOR_REPLICA_REPLICA_RATIO);
    }

    /**
     * Creates a new Composite annotator.
     * */
    public CompositeAnnotator() {
        //创建一个annotator
        mLRUClock = new AtomicLong(0);
    }

    @Override
    public BlockSortedField updateSortedField(long blockId, CompositeSortedField oldValue) {
        long clockValue = mLRUClock.incrementAndGet();
        long replicaNum =  (oldValue != null) ? oldValue.mReplicaNum : 0;
        if (LOG.isDebugEnabled()) {
            LOG.debug("ReplicaBased update for Block: {}. Clock: {}", blockId, clockValue);
        }
        return getNewSortedField(clockValue, replicaNum, oldValue);
    }

    @Override
    public BlockSortedField updateSortedFieldReplica(long blockId, CompositeSortedField oldValue, Long value) {
        long clockValue = mLRUClock.incrementAndGet();
        Long replicaNum  = (oldValue != null) ? oldValue.mReplicaNum : 0;
        if (LOG.isDebugEnabled()) {
            LOG.debug("ReplicaBased update for Block: {}. Clock: {}", blockId, clockValue);
        }
        return getNewSortedField(clockValue, replicaNum + value, oldValue);
    }

    @Override
    public void updateSortedFields(List<Pair<Long, CompositeSortedField>> blockList) {
        // Grab the current logical clock, for updating the given entries under.
        long currentClock = mLRUClock.get();
        for (Pair<Long, CompositeSortedField> blockField : blockList) {
            long replicaNum =  (blockField.getSecond() != null) ? blockField.getSecond().mReplicaNum : 0;
            blockField.setSecond(getNewSortedField(currentClock,replicaNum,blockField.getSecond()));
        }
    }

    @Override
    public void update(){
        //update COMPOSITE_RATIO
        double newRatio = Configuration.getDouble(
                PropertyKey.WORKER_BLOCK_ANNOTATOR_COMPOSITE_RATIO);
        COMPOSITE_RATIO = newRatio;
        LOG.info("The Ratio of {} updated to {}",
                PropertyKey.WORKER_BLOCK_ANNOTATOR_CLASS,PropertyKey.WORKER_BLOCK_ANNOTATOR_COMPOSITE_RATIO);
    }


    @Override
    public boolean isOnlineSorter() {
        return true;
    }

    public static void setCompositeRatio(double compositeRatio){
        COMPOSITE_RATIO = compositeRatio;
    }

    public double getCompositeRatio(){
        return COMPOSITE_RATIO;
    }

    private CompositeSortedField getNewSortedField(long clockValue, long replicaNum, CompositeSortedField oldValue) {
        double crfValue = (oldValue != null) ? oldValue.mCrfValue : 1.0;
//        long replicaNum =  (oldValue != null) ? oldValue.mReplicaNum : 0;
//        long clockValue = mLRUClock.incrementAndGet();
        if (oldValue != null && clockValue != oldValue.mClockValue) {
            //calculate LRFU value
            double crfValueLRFU = oldValue.mCrfValue * calculateAccessWeight(clockValue - oldValue.mClockValue) + 1.0;
            //calculate ReplicabasedLRU value
            double crfValueReplica = clockValue * LRU_RATIO - replicaNum * REPLICA_RATIO;;
            //composite LRFU value and ReplicabasedLRU value
            crfValue = crfValueLRFU *(1-COMPOSITE_RATIO) + crfValueReplica * COMPOSITE_RATIO;

        }

        return new CompositeAnnotator.CompositeSortedField(clockValue, replicaNum, crfValue);
    }

    private double calculateAccessWeight(long logicTimeInterval) {
        return Math.pow(1.0 / ATTENUATION_FACTOR, logicTimeInterval * STEP_FACTOR);
    }

    protected class CompositeSortedField implements BlockSortedField {
        private final long mClockValue;
        private final double mCrfValue;
        private final long mReplicaNum;



        private CompositeSortedField(long clockValue, long replicaNum, double crfValue) {
            mClockValue = clockValue;
            mCrfValue = crfValue;
            mReplicaNum = replicaNum;
        }

        @Override
        public int compareTo(BlockSortedField o) {
            Preconditions.checkState(o instanceof CompositeAnnotator.CompositeSortedField);
            return Double.compare(mCrfValue, ((CompositeAnnotator.CompositeSortedField) o).mCrfValue);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CompositeAnnotator.CompositeSortedField)) {
                return false;
            }
            return Double.compare(mCrfValue, ((CompositeAnnotator.CompositeSortedField) o).mCrfValue) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mCrfValue);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("Clock",  mClockValue)
                    .add("CRF", mCrfValue)
                    .add("Replica", mReplicaNum)
                    .toString();
        }
    }
}



















