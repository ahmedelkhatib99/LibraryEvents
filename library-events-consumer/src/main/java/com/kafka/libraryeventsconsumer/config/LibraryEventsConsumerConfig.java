package com.kafka.libraryeventsconsumer.config;

import com.kafka.libraryeventsconsumer.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;


@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";
    public static final String SUCCESS = "SUCCESS";
    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FailureService failureService;

    @Value("${topics.retry}")
    String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    }
                    else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                });

        return recoverer;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("Exception in consumerRecordRecoverer : {} ", e.getMessage(), e);
        var record = (ConsumerRecord<Integer, String>)consumerRecord;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            //recovery logic
            log.info("Inside Recovery");
            failureService.saveFailureRecord(record, e, RETRY);
        }
        else {
            //non recovery logic
            log.info("Inside Non-Recovery");
            failureService.saveFailureRecord(record, e, DEAD);
        }
    };

    public CommonErrorHandler errorHandler() {

        BackOff fixedBackOff = new FixedBackOff(1000L, 2);

        ExponentialBackOff expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1000L);
        expBackOff.setMultiplier(2);
        expBackOff.setMaxInterval(2000L);

//        DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);
//        DefaultErrorHandler errorHandler = new DefaultErrorHandler(expBackOff);
//        DefaultErrorHandler errorHandler = new DefaultErrorHandler(publishingRecoverer(), expBackOff);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(consumerRecordRecoverer, expBackOff);

//        var exceptionsToIgnoreList = List.of(IllegalArgumentException.class);
//        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in retry Listener, Exception: {} , deliveryAttempt: {} ", ex, deliveryAttempt);
        });

        return errorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}
