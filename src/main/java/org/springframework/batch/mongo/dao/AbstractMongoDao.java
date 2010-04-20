package org.springframework.batch.mongo.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import org.springframework.batch.mongo.config.Database;

import javax.inject.Inject;

/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 15, 2010
 */
public abstract class AbstractMongoDao {

    protected DB db;
    protected static final String UPDATED_EXISTING_STATUS = "updatedExisting";
    protected static final String VERSION_KEY = "version";
    protected static final String START_TIME_KEY = "startTime";
    protected static final String END_TIME_KEY = "endTime";
    protected static final String EXIT_CODE_KEY = "exitCode";
    protected static final String EXIT_MESSAGE_KEY = "exitMessage";
    protected static final String LAST_UPDATED_KEY = "lastUpdated";
    protected static final String STATUS_KEY = "status";
    protected static final String SEQUENCES_COLLECTION_NAME = "Sequences";

    @Inject
    @Database(Database.Purpose.BATCH)
    public void setDb(DB db) {
        this.db = db;
    }

    protected abstract DBCollection getCollection();

    protected Long getNexId(String name) {
        DBCollection collection = db.getCollection(SEQUENCES_COLLECTION_NAME);
        BasicDBObject sequence = new BasicDBObject("name", name);
        collection.update(sequence, new BasicDBObject("$inc", new BasicDBObject("value", 1L)), true, false);
        return (Long) collection.findOne(sequence).get("value");
    }
}
