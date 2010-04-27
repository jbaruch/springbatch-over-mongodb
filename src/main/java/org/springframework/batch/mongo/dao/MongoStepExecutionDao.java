package org.springframework.batch.mongo.dao;

import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.Date;

import static com.mongodb.BasicDBObjectBuilder.start;
import static org.springframework.batch.mongo.dao.MongoJobExecutionDao.JOB_EXECUTION_ID_KEY;
import static org.springframework.batch.mongo.dao.MongoJobExecutionDao.jobExecutionIdObj;
import static org.springframework.util.Assert.notNull;

/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 15, 2010
 */
@Repository
public class MongoStepExecutionDao extends AbstractMongoDao implements StepExecutionDao {
    protected static final String STEP_EXECUTION_ID_KEY = "stepExecutionId";
    protected static final String STEP_NAME_KEY = "stepName";
    protected static final String COMMIT_COUNT_KEY = "commitCount";
    protected static final String READ_COUNT_KEY = "readCount";
    protected static final String FILTER_COUT_KEY = "filterCout";
    protected static final String WRITE_COUNT_KEY = "writeCount";
    protected static final String READ_SKIP_COUNT_KEY = "readSkipCount";
    protected static final String WRITE_SKIP_COUNT_KEY = "writeSkipCount";
    protected static final String PROCESS_SKIP_COUT_KEY = "processSkipCout";
    protected static final String ROLLBACK_COUNT_KEY = "rollbackCount";
    private static final Logger LOG = LoggerFactory.getLogger(MongoStepExecutionDao.class);

    @PostConstruct
    public void init() {
        getCollection().ensureIndex(BasicDBObjectBuilder.start().add(STEP_EXECUTION_ID_KEY, 1).add(JOB_EXECUTION_ID_KEY, 1).get());

    }

    @Override
    public void saveStepExecution(StepExecution stepExecution) {
        Assert.isNull(stepExecution.getId(),
                "to-be-saved (not updated) StepExecution can't already have an id assigned");
        Assert.isNull(stepExecution.getVersion(),
                "to-be-saved (not updated) StepExecution can't already have a version assigned");

        validateStepExecution(stepExecution);

        stepExecution.setId(getNextId(StepExecution.class.getSimpleName()));
        stepExecution.incrementVersion(); // should be 0 now
        DBObject object = toDbObjectWithoutVersion(stepExecution);
        object.put(VERSION_KEY, stepExecution.getVersion());
        getCollection().save(object);

    }

    private DBObject toDbObjectWithoutVersion(StepExecution stepExecution) {
        return start()
                .add(STEP_EXECUTION_ID_KEY, stepExecution.getId())
                .add(STEP_NAME_KEY, stepExecution.getStepName())
                .add(JOB_EXECUTION_ID_KEY, stepExecution.getJobExecutionId())
                .add(START_TIME_KEY, stepExecution.getStartTime())
                .add(END_TIME_KEY, stepExecution.getEndTime())
                .add(STATUS_KEY, stepExecution.getStatus().toString())
                .add(COMMIT_COUNT_KEY, stepExecution.getCommitCount())
                .add(READ_COUNT_KEY, stepExecution.getReadCount())
                .add(FILTER_COUT_KEY, stepExecution.getFilterCount())
                .add(WRITE_COUNT_KEY, stepExecution.getWriteCount())
                .add(EXIT_CODE_KEY, stepExecution.getExitStatus().getExitCode())
                .add(EXIT_MESSAGE_KEY, stepExecution.getExitStatus().getExitDescription())
                .add(READ_SKIP_COUNT_KEY, stepExecution.getReadSkipCount())
                .add(WRITE_SKIP_COUNT_KEY, stepExecution.getWriteSkipCount())
                .add(PROCESS_SKIP_COUT_KEY, stepExecution.getProcessSkipCount())
                .add(ROLLBACK_COUNT_KEY, stepExecution.getRollbackCount())
                .add(LAST_UPDATED_KEY, stepExecution.getLastUpdated()).get();
    }

    @Override
    public synchronized void updateStepExecution(StepExecution stepExecution) {
        // Attempt to prevent concurrent modification errors by blocking here if
        // someone is already trying to do it.
        Integer currentVersion = stepExecution.getVersion();
        Integer newVersion = currentVersion + 1;
        DBObject object = toDbObjectWithoutVersion(stepExecution);
        object.put(VERSION_KEY, newVersion);
        getCollection().update(start()
                .add(STEP_EXECUTION_ID_KEY, stepExecution.getId())
                .add(VERSION_KEY, currentVersion).get(),
                object);

        // Avoid concurrent modifications...
        DBObject lastError = db.getLastError();
        if (!((Boolean) lastError.get(UPDATED_EXISTING_STATUS))) {
            LOG.error("Update returned status {}", lastError);
            DBObject existingStepExecution = getCollection().findOne(stepExecutionIdObj(stepExecution.getId()), new BasicDBObject(VERSION_KEY, 1));
            if (existingStepExecution == null) {
                throw new IllegalArgumentException("Can't update this stepExecution, it was never saved.");
            }
            Integer curentVersion = ((Integer) existingStepExecution.get(VERSION_KEY));
            throw new OptimisticLockingFailureException("Attempt to update job execution id="
                    + stepExecution.getId() + " with wrong version (" + currentVersion
                    + "), where current version is " + curentVersion);
        }

        stepExecution.incrementVersion();
    }


    static BasicDBObject stepExecutionIdObj(Long id) {
        return new BasicDBObject(STEP_EXECUTION_ID_KEY, id);
    }


    @Override
    public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
        return mapStepExecution(getCollection().findOne(BasicDBObjectBuilder.start()
                .add(STEP_EXECUTION_ID_KEY, stepExecutionId)
                .add(JOB_EXECUTION_ID_KEY, jobExecution.getId()).get()), jobExecution);
    }

    private StepExecution mapStepExecution(DBObject object, JobExecution jobExecution) {
        if (object == null) {
            return null;
        }
        StepExecution stepExecution = new StepExecution((String) object.get(STEP_NAME_KEY), jobExecution, ((Long) object.get(STEP_EXECUTION_ID_KEY)));
        stepExecution.setStartTime((Date) object.get(START_TIME_KEY));
        stepExecution.setEndTime((Date) object.get(END_TIME_KEY));
        stepExecution.setStatus(BatchStatus.valueOf((String) object.get(STATUS_KEY)));
        stepExecution.setCommitCount((Integer) object.get(COMMIT_COUNT_KEY));
        stepExecution.setReadCount((Integer) object.get(READ_COUNT_KEY));
        stepExecution.setFilterCount((Integer) object.get(FILTER_COUT_KEY));
        stepExecution.setWriteCount((Integer) object.get(WRITE_COUNT_KEY));
        stepExecution.setExitStatus(new ExitStatus((String) object.get(EXIT_CODE_KEY), ((String) object.get(EXIT_MESSAGE_KEY))));
        stepExecution.setReadSkipCount((Integer) object.get(READ_SKIP_COUNT_KEY));
        stepExecution.setWriteSkipCount((Integer) object.get(WRITE_SKIP_COUNT_KEY));
        stepExecution.setProcessSkipCount((Integer) object.get(PROCESS_SKIP_COUT_KEY));
        stepExecution.setRollbackCount((Integer) object.get(ROLLBACK_COUNT_KEY));
        stepExecution.setLastUpdated((Date) object.get(LAST_UPDATED_KEY));
        stepExecution.setVersion((Integer) object.get(VERSION_KEY));
        return stepExecution;

    }

    @Override
    public void addStepExecutions(JobExecution jobExecution) {
        DBCursor stepsCoursor = getCollection().find(jobExecutionIdObj(jobExecution.getId())).sort(stepExecutionIdObj(1L));
        while (stepsCoursor.hasNext()) {
            DBObject stepObject = stepsCoursor.next();
            //Calls constructor of StepExecution, which adds the step; Wow, that's unclear code!
            mapStepExecution(stepObject, jobExecution);
        }
    }

    @Override
    protected DBCollection getCollection() {
        return db.getCollection(StepExecution.class.getSimpleName());
    }

    private void validateStepExecution(StepExecution stepExecution) {
        notNull(stepExecution);
        notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
        notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
        notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
    }

}
