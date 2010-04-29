package org.springframework.batch.mongo.dao;

import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.*;

import static com.mongodb.BasicDBObjectBuilder.start;
import static org.springframework.batch.mongo.dao.MongoJobInstanceDao.JOB_INSTANCE_ID_KEY;
import static org.springframework.batch.mongo.dao.MongoJobInstanceDao.jobInstanceIdObj;

/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 15, 2010
 */
@Repository
public class MongoJobExecutionDao extends AbstractMongoDao implements JobExecutionDao {

    public static final String JOB_EXECUTION_ID_KEY = "jobExecutionId";
    private static final String CREATE_TIME_KEY = "createTime";
    private static final Logger LOG = LoggerFactory.getLogger(MongoJobExecutionDao.class);

    @PostConstruct
    public void init() {
        getCollection().ensureIndex(BasicDBObjectBuilder.start().add(JOB_EXECUTION_ID_KEY, 1).add(JOB_INSTANCE_ID_KEY, 1).get());
    }

    public void saveJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        jobExecution.incrementVersion();
        Long id = getNextId(JobExecution.class.getSimpleName());
        save(jobExecution, id);
    }

    private void save(JobExecution jobExecution, Long id) {
        jobExecution.setId(id);
        DBObject object = toDbObjectWithoutVersion(jobExecution);
        object.put(VERSION_KEY, jobExecution.getVersion());
        getCollection().save(object);
    }

    private DBObject toDbObjectWithoutVersion(JobExecution jobExecution) {
        return start()
                .add(JOB_EXECUTION_ID_KEY, jobExecution.getId())
                .add(JOB_INSTANCE_ID_KEY, jobExecution.getJobId())
                .add(START_TIME_KEY, jobExecution.getStartTime())
                .add(END_TIME_KEY, jobExecution.getEndTime())
                .add(STATUS_KEY, jobExecution.getStatus().toString())
                .add(EXIT_CODE_KEY, jobExecution.getExitStatus().getExitCode())
                .add(EXIT_MESSAGE_KEY, jobExecution.getExitStatus().getExitDescription())
                .add(CREATE_TIME_KEY, jobExecution.getCreateTime())
                .add(LAST_UPDATED_KEY, jobExecution.getLastUpdated()).get();
    }


    private void validateJobExecution(JobExecution jobExecution) {

        Assert.notNull(jobExecution);
        Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
        Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
        Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
    }

    public synchronized void updateJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);

        Long jobExecutionId = jobExecution.getId();
        Assert.notNull(jobExecutionId,
                "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");

        Assert.notNull(jobExecution.getVersion(),
                "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

        Integer version = jobExecution.getVersion() + 1;

        if (getCollection().findOne(jobExecutionIdObj(jobExecutionId)) == null) {
            throw new NoSuchObjectException("Invalid JobExecution, ID " + jobExecutionId + " not found.");
        }

        DBObject object = toDbObjectWithoutVersion(jobExecution);
        object.put(VERSION_KEY, version);
        getCollection().update(start()
                .add(JOB_EXECUTION_ID_KEY, jobExecutionId)
                .add(VERSION_KEY, jobExecution.getVersion()).get(),
                object);

        // Avoid concurrent modifications...
        DBObject lastError = db.getLastError();
        if (!((Boolean) lastError.get(UPDATED_EXISTING_STATUS))) {
            LOG.error("Update returned status {}", lastError);
            DBObject existingJobExecution = getCollection().findOne(jobExecutionIdObj(jobExecutionId), new BasicDBObject(VERSION_KEY, 1));
            if (existingJobExecution == null) {
                throw new IllegalArgumentException("Can't update this jobExecution, it was never saved.");
            }
            Integer curentVersion = ((Integer) existingJobExecution.get(VERSION_KEY));
            throw new OptimisticLockingFailureException("Attempt to update job execution id="
                    + jobExecutionId + " with wrong version (" + jobExecution.getVersion()
                    + "), where current version is " + curentVersion);
        }

        jobExecution.incrementVersion();
    }

    public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
        Assert.notNull(jobInstance, "Job cannot be null.");
        Long id = jobInstance.getId();
        Assert.notNull(id, "Job Id cannot be null.");
        DBCursor dbCursor = getCollection().find(jobInstanceIdObj(id)).sort(new BasicDBObject(JOB_EXECUTION_ID_KEY, -1));
        List<JobExecution> result = new ArrayList<JobExecution>();
        while (dbCursor.hasNext()) {
            DBObject dbObject = dbCursor.next();
            result.add(mapJobExecution(jobInstance, dbObject));
        }
        return result;
    }

    public JobExecution getLastJobExecution(JobInstance jobInstance) {
        Long id = jobInstance.getId();

        DBCursor dbCursor = getCollection().find(jobInstanceIdObj(id)).sort(new BasicDBObject(CREATE_TIME_KEY, -1)).limit(1);
        if (!dbCursor.hasNext()) {
            return null;
        } else {
            DBObject singleResult = dbCursor.next();
            if (dbCursor.hasNext()) {
                throw new IllegalStateException("There must be at most one latest job execution");
            }
            return mapJobExecution(jobInstance, singleResult);
        }
    }

    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        DBCursor instancesCursor = db.getCollection(JobInstance.class.getSimpleName()).find(new BasicDBObject(MongoJobInstanceDao.JOB_NAME_KEY, jobName), jobInstanceIdObj(1L));
        List<Long> ids = new ArrayList<Long>();
        while (instancesCursor.hasNext()) {
            ids.add((Long) instancesCursor.next().get(JOB_INSTANCE_ID_KEY));
        }

        DBCursor dbCursor = getCollection().find(BasicDBObjectBuilder.start()
                .add(JOB_INSTANCE_ID_KEY, new BasicDBObject("$in", ids.toArray()))
                .add(END_TIME_KEY, null).get()).sort(jobExecutionIdObj(-1L));
        Set<JobExecution> result = new HashSet<JobExecution>();
        while (dbCursor.hasNext()) {
            result.add(mapJobExecution(dbCursor.next()));
        }
        return result;
    }

    public JobExecution getJobExecution(Long executionId) {
        return mapJobExecution(getCollection().findOne(jobExecutionIdObj(executionId)));
    }

    public void synchronizeStatus(JobExecution jobExecution) {
        Long id = jobExecution.getId();
        DBObject jobExecutionObject = getCollection().findOne(jobExecutionIdObj(id));
        int currentVersion = jobExecutionObject != null ? ((Integer) jobExecutionObject.get(VERSION_KEY)) : 0;
        if (currentVersion != jobExecution.getVersion()) {
            if (jobExecutionObject == null) {
                save(jobExecution, id);
                jobExecutionObject = getCollection().findOne(jobExecutionIdObj(id));
            }
            String status = (String) jobExecutionObject.get(STATUS_KEY);
            jobExecution.upgradeStatus(BatchStatus.valueOf(status));
            jobExecution.setVersion(currentVersion);
        }
    }

    static BasicDBObject jobExecutionIdObj(Long id) {
        return new BasicDBObject(JOB_EXECUTION_ID_KEY, id);
    }

    @Override
    protected DBCollection getCollection() {
        return db.getCollection(JobExecution.class.getSimpleName());
    }

    private JobExecution mapJobExecution(DBObject dbObject) {
        return mapJobExecution(null, dbObject);
    }

    private JobExecution mapJobExecution(JobInstance jobInstance, DBObject dbObject) {
        if (dbObject == null) {
            return null;
        }
        Long id = (Long) dbObject.get(JOB_EXECUTION_ID_KEY);
        JobExecution jobExecution;

        if (jobInstance == null) {
            jobExecution = new JobExecution(id);
        } else {
            jobExecution = new JobExecution(jobInstance, id);
        }
        jobExecution.setStartTime((Date) dbObject.get(START_TIME_KEY));
        jobExecution.setEndTime((Date) dbObject.get(END_TIME_KEY));
        jobExecution.setStatus(BatchStatus.valueOf((String) dbObject.get(STATUS_KEY)));
        jobExecution.setExitStatus(new ExitStatus(((String) dbObject.get(EXIT_CODE_KEY)), (String) dbObject.get(EXIT_MESSAGE_KEY)));
        jobExecution.setCreateTime((Date) dbObject.get(CREATE_TIME_KEY));
        jobExecution.setLastUpdated((Date) dbObject.get(LAST_UPDATED_KEY));
        jobExecution.setVersion((Integer) dbObject.get(VERSION_KEY));
        return jobExecution;
    }


}
