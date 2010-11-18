package org.springframework.batch.mongo.example;

import org.springframework.batch.item.ItemReader;
import org.springframework.stereotype.Component;

/**
 * {@link ItemReader} with hard-coded input data.
 */
@Component
public class ExampleItemReader implements ItemReader<String> {

    private String[] input = {"Hello world!", null};

    private int index = 0;

    /**
     * Reads next record from input
     */
    public String read() throws Exception {
        if (index < input.length) {
            return input[index++];
        } else {
            return null;
        }

    }

}
