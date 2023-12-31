package unit.com.kafka.libraryeventsproducer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.libraryeventsproducer.LibraryEventsProducerApplication;
import com.kafka.libraryeventsproducer.controller.LibraryEventController;
import com.kafka.libraryeventsproducer.domain.LibraryEvent;
import com.kafka.libraryeventsproducer.producer.LibraryEventsProducer;
import integ.com.kafka.libraryeventsproducer.util.TestUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@WebMvcTest(LibraryEventController.class)
@ContextConfiguration(classes = LibraryEventsProducerApplication.class)
class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {

        when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(TestUtil.libraryEventRecord())))
                .andExpect(status().isCreated());
    }

    @Test
    void postLibraryEventInvalidValues() throws Exception {

        when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook())))
                .andExpect(status().is4xxClientError());
    }


}