package org.springframework.batch.mongo.config;

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.UnknownHostException;

/**
 * com.mongodb.DB object factory bean for usage in Spring xml configuration.
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 28, 2010
 */
public class MongoDbFactoryBean implements FactoryBean, InitializingBean {

    private String dbName;
    private Mongo mongo;
    private DB db;

    public MongoDbFactoryBean(String dbName, String mongoHost, int mongoPort) throws UnknownHostException {
        this.dbName = dbName;
        this.mongo = new Mongo(mongoHost, mongoPort);
    }

    public Object getObject() throws Exception {
        return db;
    }

    public Class getObjectType() {
        return DB.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void afterPropertiesSet() throws Exception {
        db = mongo.getDB(dbName);
    }
}
