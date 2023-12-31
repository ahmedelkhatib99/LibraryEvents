package com.kafka.libraryeventsconsumer.schedule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.libraryeventsconsumer.config.LibraryEventsConsumerConfig;
import com.kafka.libraryeventsconsumer.entity.FailureRecord;
import com.kafka.libraryeventsconsumer.jpa.FailureRecordRepository;
import com.kafka.libraryeventsconsumer.service.FailureService;
import com.kafka.libraryeventsconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class RetryScheduler {

    private FailureRecordRepository failureRecordRepository;
    private LibraryEventsService libraryEventsService;

    public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventsService libraryEventsService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventsService = libraryEventsService;
    }

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {

        log.info("Retrying faild records started");

        failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY)
                .forEach(failureRecord -> {
                    log.info("Retrying faildRecord: {} ", failureRecord);
                    ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                        failureRecordRepository.save(failureRecord);
                    } catch (Exception e) {
                        log.info("Exception in retryFailedRecords: {} ", e);
                    }
                });

        log.info("Retrying faild records completed");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKey_value(),
                failureRecord.getErrorRecord());
    }
}
