package org.springframework.batch.mongo.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.nio.BufferUnderflowException;

/**
 * Created by IntelliJ IDEA.
 *
 * @author JBaruch
 * @since 28-Apr-2010
 */
public class Shutdown {
    public static void main(String[] args) {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("application-config.xml");
        Mongo mongo = ctx.getBean(Mongo.class);
        DB adminDb = mongo.getDB("admin");
        try {
            adminDb.command(new BasicDBObject("shutdown", Boolean.TRUE));
        } catch (BufferUnderflowException ignored) {
        } finally {
            ctx.close();
        }
    }
}
