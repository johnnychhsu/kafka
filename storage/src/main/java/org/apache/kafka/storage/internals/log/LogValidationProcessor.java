package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public interface LogValidationProcessor {
    void validateIBP();
    void decideInplaceAssignment();
    void validateBatch();
    void validateRecord();
    LogValidator.ValidationResult validate();
}

class NonCompressedRecordBatchMagicV2Processor implements LogValidationProcessor {

    private byte magic;
    private CompressionType sourceCompressionType;
    private CompressionType targetCompressionType;
    private AppendOrigin origin;
    private MemoryRecords records;
    private TimestampType timestampType;
    private LogValidator.MetricsRecorder metricsRecorder;
    private TopicPartition topicPartition;
    private PrimitiveRef.LongRef offsetCounter;
    private Time time;

    public NonCompressedRecordBatchMagicV2Processor(byte magic, CompressionType sourceCompressionType,
                                                    CompressionType targetCompressionType, AppendOrigin origin,
                                                    MemoryRecords records, TimestampType timestampType) {
        this.magic = magic;
        this.sourceCompressionType = sourceCompressionType;
        this.targetCompressionType = targetCompressionType;
        this.origin = origin;
        this.records = records;
        this.timestampType = timestampType;
    }

    // this can be put in abstract class for sharing as well
    private static class ApiRecordError {
        final Errors apiError;
        final ProduceResponse.RecordError recordError;

        private ApiRecordError(Errors apiError, ProduceResponse.RecordError recordError) {
            this.apiError = apiError;
            this.recordError = recordError;
        }
    }
    @Override
    public void validateIBP() {}

    @Override
    public void decideInplaceAssignment() {}

    @Override
    public void validateBatch(long timestampBeforeMaxMs, long timestampAfterMaxMs) {
        long now = time.milliseconds();
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;
        long initialOffset = offsetCounter.value;
        RecordBatch firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, CompressionType.NONE);
        for (MutableRecordBatch batch : records.batches()) {
            validateBatch(topicPartition, firstBatch, batch, origin, metricsRecorder);
            long maxBatchTimestamp = RecordBatch.NO_TIMESTAMP;
            long offsetOfMaxBatchTimestamp = -1L;

            List<ApiRecordError> recordErrors = new ArrayList<>(0);
            // This is a hot path and we want to avoid any unnecessary allocations.
            // That said, there is no benefit in using `skipKeyValueIterator` for the uncompressed
            // case since we don't do key/value copies in this path (we just slice the ByteBuffer)
            int recordIndex = 0;
            for (Record record : batch) {
                Optional<ApiRecordError> recordError = validateRecord(batch, topicPartition, record,
                        recordIndex, now, timestampType, timestampBeforeMaxMs, timestampAfterMaxMs, compactedTopic, metricsRecorder);
                recordError.ifPresent(recordErrors::add);

                long offset = offsetCounter.value++;
                if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && record.timestamp() > maxBatchTimestamp) {
                    maxBatchTimestamp = record.timestamp();
                    offsetOfMaxBatchTimestamp = offset;
                }
                ++recordIndex;
            }

            processRecordErrors(recordErrors);

            if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
                maxTimestamp = maxBatchTimestamp;
                offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp;
            }

            batch.setLastOffset(offsetCounter.value - 1);
            batch.setPartitionLeaderEpoch(partitionLeaderEpoch);

            if (timestampType == TimestampType.LOG_APPEND_TIME)
                batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now);
            else
                batch.setMaxTimestamp(timestampType, maxBatchTimestamp);
        }
    }

    @Override
    public void validateRecord() {

    }

    @Override
    public LogValidator.ValidationResult validate() {
        return null;
    }

    // this can be further extracted into abstract class for share
    private static MutableRecordBatch getFirstBatchAndMaybeValidateNoMoreBatches(MemoryRecords records,
                                                                                 CompressionType sourceCompression) {
        Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();

        if (!batchIterator.hasNext())
            throw new InvalidRecordException("Record batch has no batches at all");

        MutableRecordBatch batch = batchIterator.next();

        // if the format is v2 and beyond, or if the messages are compressed, we should check there's only one batch.
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || sourceCompression != CompressionType.NONE) {
            if (batchIterator.hasNext())
                throw new InvalidRecordException("Compressed outer record has more than one batch");
        }

        return batch;
    }

    private static void validateBatch(TopicPartition topicPartition,
                                      RecordBatch firstBatch,
                                      RecordBatch batch,
                                      AppendOrigin origin,
                                      LogValidator.MetricsRecorder metricsRecorder) {
        // batch magic byte should have the same magic as the first batch
        if (firstBatch.magic() != batch.magic()) {
            metricsRecorder.recordInvalidMagic();
            throw new InvalidRecordException("Batch magic " + batch.magic() + " is not the same as the first batch's magic byte "
                    + firstBatch.magic() + " in topic partition " + topicPartition);
        }

        if (origin == AppendOrigin.CLIENT) {

            long countFromOffsets = batch.lastOffset() - batch.baseOffset() + 1;
            if (countFromOffsets <= 0) {
                metricsRecorder.recordInvalidOffset();
                throw new InvalidRecordException("Batch has an invalid offset range: [" + batch.baseOffset() + ", "
                        + batch.lastOffset() + "] in topic partition " + topicPartition);
            }

            // v2 and above messages always have a non-null count
            long count = batch.countOrNull();
            if (count <= 0) {
                metricsRecorder.recordInvalidOffset();
                throw new InvalidRecordException("Invalid reported count for record batch: " + count
                        + " in topic partition " + topicPartition);
            }

            if (countFromOffsets != count) {
                metricsRecorder.recordInvalidOffset();
                throw new InvalidRecordException("Inconsistent batch offset range [" + batch.baseOffset() + ", "
                        + batch.lastOffset() + "] and count of records " + count + " in topic partition " + topicPartition);
            }


            if (batch.isControlBatch()) {
                metricsRecorder.recordInvalidOffset();
                throw new InvalidRecordException("Clients are not allowed to write control records in topic partition " + topicPartition);
            }

            if (batch.hasProducerId() && batch.baseSequence() < 0) {
                metricsRecorder.recordInvalidSequence();
                throw new InvalidRecordException("Invalid sequence number " + batch.baseSequence() + " in record batch with producerId "
                        + batch.producerId() + " in topic partition " + topicPartition);
            }
        }

    }
}
