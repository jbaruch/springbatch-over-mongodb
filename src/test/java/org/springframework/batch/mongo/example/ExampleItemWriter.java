package org.springframework.batch.mongo.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;


/**
 * Dummy {@link ItemWriter} which only logs data it receives.
 */
@Component
public class ExampleItemWriter implements ItemWriter<Object> {

    private static final Log log = LogFactory.getLog(ExampleItemWriter.class);

    /**
     * @see ItemWriter#write(List)
     */
    public void write(List<?> data) throws Exception {
        log.info(data);
    }

}
