package com.kafka.libraryeventsconsumer.service;

import com.kafka.libraryeventsconsumer.entity.FailureRecord;
import com.kafka.libraryeventsconsumer.jpa.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {

    FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailureRecord(ConsumerRecord<Integer, String> record, Exception e, String status) {
        FailureRecord failureRecord = new FailureRecord(null, record.topic(), record.key(), record.value(), record.partition(), record.offset(), e.getCause().getMessage(), status);
        failureRecordRepository.save(failureRecord);
    }
}
