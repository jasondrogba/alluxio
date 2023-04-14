package alluxio.worker.block.annotator;

import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import  alluxio.worker.block.annotator.LRFUAnnotator;
import alluxio.worker.block.annotator.ReplicaBasedAnnotator;
public class CompositeAnnotator implements BlockAnnotator<CompositeAnnotator.CompositeSortedField> {
    private static final Logger LOG = LoggerFactory.getLogger(CompositeAnnotator.class);

    private static final double COMPOSITE_RATIO;
    /** In the range of [0, 1]. Closer to 0, LRFU closer to LFU. Closer to 1, LRFU closer to LRU. */
    private static final double STEP_FACTOR;
    /** The attenuation factor is in the range of [2, INF]. */
    private static final double ATTENUATION_FACTOR;
    private static final double LRU_RATIO;
    private static final double REPLICA_RATIO;
    private final AtomicLong mLRUClock = new AtomicLong();
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
    }

    @Override
    public BlockSortedField updateSortedField(long blockId, CompositeAnnotator.CompositeSortedField oldValue) {
        return getNewSortedField(blockId, oldValue, mLRUClock.incrementAndGet());
    }

    @Override
    public BlockSortedField updateSortedFieldReplica(long blockId, CompositeSortedField oldValue, Long value) {
        return getNewSortedField(blockId, oldValue, mLRUClock.incrementAndGet());
    }

    @Override
    public void updateSortedFields(List<Pair<Long, CompositeSortedField>> blockList) {
        // Grab the current logical clock, for updating the given entries under.
        long clockValue = mLRUClock.get();
        for (Pair<Long, CompositeAnnotator.CompositeSortedField> blockField : blockList) {
            blockField.setSecond(getNewSortedField(
                    blockField.getFirst(), blockField.getSecond(), clockValue));
        }
    }


    @Override
    public boolean isOnlineSorter() {
        return true;
    }

    private CompositeAnnotator.CompositeSortedField getNewSortedField(long blockId, CompositeAnnotator.CompositeSortedField oldValue, long clock) {
        double crfValue = (oldValue != null) ? oldValue.mCrfValue : 1.0;
        long replicaNum =  (oldValue != null) ? oldValue.mReplicaNum : 0;
        long clockValue = mLRUClock.incrementAndGet();
        if (oldValue != null && clock != oldValue.mClockValue) {
            //calculate LRFU value
            double crfValueLRFU = oldValue.mCrfValue * calculateAccessWeight(clock - oldValue.mClockValue) + 1.0;
            //calculate ReplicabasedLRU value
            double crfValueReplica = clockValue * LRU_RATIO - replicaNum * REPLICA_RATIO;;
            //composite LRFU value and ReplicabasedLRU value
            crfValue = crfValueLRFU *(1-COMPOSITE_RATIO) + crfValueReplica * COMPOSITE_RATIO;

        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Composite update for Block: {}. Clock:{}, CRF: {}", blockId, clock, crfValue);
        }

        return new CompositeAnnotator.CompositeSortedField(clock, crfValue,replicaNum);
    }

    private double calculateAccessWeight(long logicTimeInterval) {
        return Math.pow(1.0 / ATTENUATION_FACTOR, logicTimeInterval * STEP_FACTOR);
    }

    protected class CompositeSortedField implements BlockSortedField {
        private final long mClockValue;
        private final double mCrfValue;
        private final Long mReplicaNum;



        private CompositeSortedField(long clockValue, double crfValue, long replicaNum) {
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



















