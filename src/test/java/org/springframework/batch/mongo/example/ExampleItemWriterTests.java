package org.springframework.batch.mongo.example;

import org.junit.Test;

public class ExampleItemWriterTests {

    private ExampleItemWriter writer = new ExampleItemWriter();

    @Test
    public void testWrite() throws Exception {
        writer.write(null); // nothing bad happens
    }

}
