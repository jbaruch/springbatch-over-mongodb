package org.springframework.batch.mongo.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:application-config.xml"})
public class MongoJobDaoTests extends AbstractJobDaoTests {

    public static final String LONG_STRING = "A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String A very long String ";

    @Transactional
    @Test
    public void testUpdateJobExecutionWithLongExitCode() {

        jobExecution.setExitStatus(ExitStatus.COMPLETED
                .addExitDescription(LONG_STRING));
        jobExecutionDao.updateJobExecution(jobExecution);


        DBObject dbObject = db.getCollection(JobExecution.class.getSimpleName()).findOne(new BasicDBObject(MongoJobInstanceDao.JOB_INSTANCE_ID_KEY, jobInstance.getId()));
        assertEquals(LONG_STRING, dbObject.get(AbstractMongoDao.EXIT_MESSAGE_KEY));
    }

}