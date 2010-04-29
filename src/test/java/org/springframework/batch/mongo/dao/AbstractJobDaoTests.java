/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.mongo.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.batch.mongo.config.Database;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;
import static org.springframework.batch.mongo.dao.AbstractMongoDao.VERSION_KEY;
import static org.springframework.batch.mongo.dao.MongoJobExecutionDao.JOB_EXECUTION_ID_KEY;
import static org.springframework.batch.mongo.dao.MongoJobInstanceDao.JOB_INSTANCE_ID_KEY;
import static org.springframework.batch.mongo.dao.MongoJobInstanceDao.JOB_NAME_KEY;

/**
 * @author Dave Syer
 */
public abstract class AbstractJobDaoTests {

    protected JobInstanceDao jobInstanceDao;

    protected JobExecutionDao jobExecutionDao;

    protected JobParameters jobParameters = new JobParametersBuilder().addString("job.key", "jobKey").addLong("long",
            (long) 1).addDate("date", new Date(7)).addDouble("double", 7.7).toJobParameters();

    protected JobInstance jobInstance;

    protected String jobName = "Job1";

    protected JobExecution jobExecution;

    protected Date jobExecutionStartTime = new Date(System.currentTimeMillis());

    @Autowired
    @Database(Database.Purpose.BATCH)
    protected DB db;

    /*
      * Because AbstractTransactionalSpringContextTests is used, this method will
      * be called by Spring to set the JobRepository.
      */

    @Autowired
    public void setJobInstanceDao(JobInstanceDao jobInstanceDao) {
        this.jobInstanceDao = jobInstanceDao;
    }

    @Autowired
    public void setJobExecutionDao(JobExecutionDao jobExecutionDao) {
        this.jobExecutionDao = jobExecutionDao;
    }

    @Before
    public void onSetUpInTransaction() throws Exception {
        db.dropDatabase();
        // Create job.
        jobInstance = jobInstanceDao.createJobInstance(jobName, jobParameters);

        // Create an execution
        jobExecutionStartTime = new Date(System.currentTimeMillis());
        jobExecution = new JobExecution(jobInstance);
        jobExecution.setStartTime(jobExecutionStartTime);
        jobExecution.setStatus(BatchStatus.STARTED);
        jobExecutionDao.saveJobExecution(jobExecution);
    }

    @Transactional
    @Test
    public void testVersionIsNotNullForJob() throws Exception {
        assertEquals(0, getVersion(JobInstance.class.getSimpleName(), jobInstance.getId(), JOB_INSTANCE_ID_KEY));
    }

    private int getVersion(String collectionName, Long id, String idKey) {
        DBObject dbObject = db.getCollection(collectionName).findOne(new BasicDBObject(idKey, id), new BasicDBObject(VERSION_KEY, 1));
        return dbObject == null ? 0 : (Integer) dbObject.get(VERSION_KEY);
    }

    @Transactional
    @Test
    public void testVersionIsNotNullForJobExecution() throws Exception {
        assertEquals(0, getVersion(JobExecution.class.getSimpleName(), jobExecution.getId(), JOB_EXECUTION_ID_KEY));
    }

    @Transactional
    @Test
    public void testFindNonExistentJob() {
        // No job should be found since it hasn't been created.
        JobInstance jobInstance = jobInstanceDao.getJobInstance("nonexistentJob", jobParameters);
        assertNull(jobInstance);
    }

    @Transactional
    @Test
    public void testFindJob() {

        JobInstance instance = jobInstanceDao.getJobInstance(jobName, jobParameters);
        assertNotNull(instance);
        assertTrue(jobInstance.equals(instance));
        assertEquals(jobParameters, instance.getJobParameters());
    }

    @Transactional
    @Test(expected = IllegalArgumentException.class)
    public void testFindJobWithNullRuntime() {
        jobInstanceDao.getJobInstance(null, null);
    }

    /**
     * Test that ensures that if you create a job with a given name, then find a
     * job with the same name, but other pieces of the identifier different, you
     * get no result, not the existing one.
     */
    @Transactional
    @Test
    public void testCreateJobWithExistingName() {

        String scheduledJob = "ScheduledJob";
        jobInstanceDao.createJobInstance(scheduledJob, jobParameters);

        // Modifying the key should bring back a completely different
        // JobInstance
        JobParameters tempProps = new JobParametersBuilder().addString("job.key", "testKey1").toJobParameters();

        JobInstance instance;
        instance = jobInstanceDao.getJobInstance(scheduledJob, jobParameters);
        assertNotNull(instance);
        assertEquals(jobParameters, instance.getJobParameters());

        instance = jobInstanceDao.getJobInstance(scheduledJob, tempProps);
        assertNull(instance);

    }

    @Transactional
    @Test
    public void testUpdateJobExecution() {

        jobExecution.setStatus(BatchStatus.COMPLETED);
        jobExecution.setExitStatus(ExitStatus.COMPLETED);
        jobExecution.setEndTime(new Date(System.currentTimeMillis()));
        jobExecutionDao.updateJobExecution(jobExecution);

        List<JobExecution> executions = jobExecutionDao.findJobExecutions(jobInstance);
        assertEquals(executions.size(), 1);
        validateJobExecution(jobExecution, executions.get(0));

    }

    @Transactional
    @Test
    public void testSaveJobExecution() {

        List<JobExecution> executions = jobExecutionDao.findJobExecutions(jobInstance);
        assertEquals(executions.size(), 1);
        validateJobExecution(jobExecution, executions.get(0));
    }

    @Transactional
    @Test(expected = NoSuchObjectException.class)
    public void testUpdateInvalidJobExecution() {
        // id is invalid
        JobExecution execution = new JobExecution(jobInstance, (long) 29432);
        execution.incrementVersion();
        jobExecutionDao.updateJobExecution(execution);
    }

    @Transactional
    @Test(expected = IllegalArgumentException.class)
    public void testUpdateNullIdJobExection() {
        JobExecution execution = new JobExecution(jobInstance);
        jobExecutionDao.updateJobExecution(execution);
    }


    @Transactional
    @Test
    public void testJobWithSimpleJobIdentifier() throws Exception {

        String testJob = "test";
        // Create job.
        jobInstance = jobInstanceDao.createJobInstance(testJob, jobParameters);
        DBObject dbObject = db.getCollection(JobInstance.class.getSimpleName()).findOne(new BasicDBObject(JOB_INSTANCE_ID_KEY, jobInstance.getId()));
        assertEquals("test", dbObject.get(JOB_NAME_KEY));
    }

    @Transactional
    @Test
    public void testJobWithDefaultJobIdentifier() throws Exception {

        String testDefaultJob = "testDefault";
        // Create job.
        jobInstance = jobInstanceDao.createJobInstance(testDefaultJob, jobParameters);

        JobInstance instance = jobInstanceDao.getJobInstance(testDefaultJob, jobParameters);

        assertNotNull(instance);
        assertEquals(jobParameters.getString("job.key"), instance.getJobParameters().getString(
                "job.key"));

    }

    @Transactional
    @Test
    public void testFindJobExecutions() {

        List<JobExecution> results = jobExecutionDao.findJobExecutions(jobInstance);
        assertEquals(results.size(), 1);
        validateJobExecution(jobExecution, results.get(0));
    }

    @Transactional
    @Test
    public void testFindJobsWithProperties() throws Exception {

    }

    private void validateJobExecution(JobExecution lhs, JobExecution rhs) {

        // equals operator only checks id
        assertEquals(lhs, rhs);
        assertEquals(lhs.getStartTime(), rhs.getStartTime());
        assertEquals(lhs.getEndTime(), rhs.getEndTime());
        assertEquals(lhs.getStatus(), rhs.getStatus());
        assertEquals(lhs.getExitStatus(), rhs.getExitStatus());
    }

    @Transactional
    @Test
    public void testGetLastJobExecution() {
        JobExecution lastExecution = new JobExecution(jobInstance);
        lastExecution.setStatus(BatchStatus.STARTED);

        int JUMP_INTO_FUTURE = 1000; // makes sure start time is 'greatest'
        lastExecution.setCreateTime(new Date(System.currentTimeMillis() + JUMP_INTO_FUTURE));
        jobExecutionDao.saveJobExecution(lastExecution);

        assertEquals(lastExecution, jobExecutionDao.getLastJobExecution(jobInstance));
    }

    /**
     * Trying to create instance twice for the same job+parameters causes error
     */
    @Transactional
    @Test(expected = IllegalStateException.class)
    public void testCreateDuplicateInstance() {
        jobParameters = new JobParameters();
        jobInstanceDao.createJobInstance(jobName, jobParameters);
        jobInstanceDao.createJobInstance(jobName, jobParameters);
    }

    @Transactional
    @Test
    public void testCreationAddsVersion() {
        jobInstance = jobInstanceDao.createJobInstance("testCreationAddsVersion", new JobParameters());
        assertNotNull(jobInstance.getVersion());
    }

    @Transactional
    @Test
    public void testSaveAddsVersionAndId() {

        JobExecution jobExecution = new JobExecution(jobInstance);

        assertNull(jobExecution.getId());
        assertNull(jobExecution.getVersion());

        jobExecutionDao.saveJobExecution(jobExecution);

        assertNotNull(jobExecution.getId());
        assertNotNull(jobExecution.getVersion());
    }

    @Transactional
    @Test
    public void testUpdateIncrementsVersion() {
        int version = jobExecution.getVersion();

        jobExecutionDao.updateJobExecution(jobExecution);

        assertEquals(version + 1, jobExecution.getVersion().intValue());
    }
}
