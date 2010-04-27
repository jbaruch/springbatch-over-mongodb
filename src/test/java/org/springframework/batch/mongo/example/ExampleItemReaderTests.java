package org.springframework.batch.mongo.example;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class ExampleItemReaderTests {

    private ExampleItemReader reader = new ExampleItemReader();

    @Test
    public void testReadOnce() throws Exception {
        assertEquals("Hello world!", reader.read());
    }

    @Test
    public void testReadTwice() throws Exception {
        reader.read();
        assertEquals(null, reader.read());
    }

}
