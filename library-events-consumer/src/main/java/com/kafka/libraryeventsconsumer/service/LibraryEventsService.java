package com.kafka.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.libraryeventsconsumer.entity.LibraryEvent;
import com.kafka.libraryeventsconsumer.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    private ObjectMapper objectMapper;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Library Event: {} ", libraryEvent);

        if(libraryEvent.getLibraryEventId() != null && (libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId() == 999 )){
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library Event Id is Missing");
        }

        Optional<LibraryEvent> existingLibraryEvent = this.libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if (!existingLibraryEvent.isPresent()) {
            throw new IllegalArgumentException("Library Event Does Not Exist");
        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        this.libraryEventsRepository.save(libraryEvent);
        log.info("Saved Successfully Library Event: {} ", libraryEvent);
    }

}
