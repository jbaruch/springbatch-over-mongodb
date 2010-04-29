package org.springframework.batch.mongo.dao;

import com.mongodb.DB;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.mongo.config.Database;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * Created by IntelliJ IDEA.
 *
 * @author JBaruch
 * @since 20-Apr-2010
 */
@ContextConfiguration(locations = {"classpath:application-config.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
public class MongoDaoTests {

    @Autowired
    @Database(Database.Purpose.BATCH)
    private DB batchDB;

    @Autowired
    private MongoJobExecutionDao dao;

    @Before
    public void setUp() throws Exception {
        batchDB.dropDatabase();
    }

    @Test
    public void testGetNextId() {
        for (long i = 1; i <= 100; i++) {
            long id = dao.getNextId(MongoJobExecutionDao.class.getSimpleName());
            Assert.assertEquals(i, id);

        }
    }
}
