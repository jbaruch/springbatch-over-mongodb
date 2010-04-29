package org.springframework.batch.mongo.dao;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.math.BigInteger;
import java.security.MessageDigest;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:application-config.xml"})
public class MongoJobInstanceDaoTests extends AbstractJobInstanceDaoTests {

    @Autowired
    private JobExecutionDao jobExecutionDao;
    @Autowired
    private JobInstanceDao jobInstanceDao;

    protected JobInstanceDao getJobInstanceDao() {
        return jobInstanceDao;
    }

    @Test
    public void testFindJobInstanceByExecution() {

        JobInstance jobInstance = dao.createJobInstance("testInstance",
                new JobParameters());
        JobExecution jobExecution = new JobExecution(jobInstance, 2L);
        jobExecutionDao.saveJobExecution(jobExecution);

        JobInstance returnedInstance = dao.getJobInstance(jobExecution);
        assertEquals(jobInstance, returnedInstance);
    }

    @Test
    public void testCreateJobKey() {

        MongoJobInstanceDao jdbcDao = (MongoJobInstanceDao) dao;
        JobParameters jobParameters = new JobParametersBuilder().addString(
                "foo", "bar").addString("bar", "foo").toJobParameters();
        String key = jdbcDao.createJobKey(jobParameters);
        assertEquals(32, key.length());

    }

    @Test
    public void testCreateJobKeyOrdering() {

        MongoJobInstanceDao jdbcDao = (MongoJobInstanceDao) dao;
        JobParameters jobParameters1 = new JobParametersBuilder().addString(
                "foo", "bar").addString("bar", "foo").toJobParameters();
        String key1 = jdbcDao.createJobKey(jobParameters1);
        JobParameters jobParameters2 = new JobParametersBuilder().addString(
                "bar", "foo").addString("foo", "bar").toJobParameters();
        String key2 = jdbcDao.createJobKey(jobParameters2);
        assertEquals(key1, key2);

    }

    @Test
    public void testHexing() throws Exception {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] bytes = digest.digest("f78spx".getBytes("UTF-8"));
        StringBuffer output = new StringBuffer();
        for (byte bite : bytes) {
            output.append(String.format("%02x", bite));
        }
        assertEquals("Wrong hash: " + output, 32, output.length());
        String value = String.format("%032x", new BigInteger(1, bytes));
        assertEquals("Wrong hash: " + value, 32, value.length());
        assertEquals(value, output.toString());
    }
}