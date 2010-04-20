package org.springframework.batch.mongo.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 15, 2010
 */
@Repository
public class MongoExecutionContextDao extends AbstractMongoDao implements ExecutionContextDao {
    private static final String STEP_EXECUTION_ID_KEY = "stepExecutionId";
    private static final String JOB_EXECUTION_ID_KEY = "jobExecutionId";

    @PostConstruct
    public void init() {
        getCollection().ensureIndex(BasicDBObjectBuilder.start().add(STEP_EXECUTION_ID_KEY, 1).add(JOB_EXECUTION_ID_KEY, 1).get());
    }

    @Override
    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        return getExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId());
    }

    @Override
    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        return getExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId());
    }

    @Override
    public void saveExecutionContext(JobExecution jobExecution) {
        saveOrUpdateExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId(), jobExecution.getExecutionContext());
    }

    @Override
    public void saveExecutionContext(StepExecution stepExecution) {
        saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
    }

    @Override
    public void updateExecutionContext(JobExecution jobExecution) {
        saveOrUpdateExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId(), jobExecution.getExecutionContext());
    }

    @Override
    public void updateExecutionContext(StepExecution stepExecution) {
        saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
    }

    private void saveOrUpdateExecutionContext(String executionIdKey, Long executionId, ExecutionContext executionContext) {
        Assert.notNull(executionId, "ExecutionId must not be null.");
        Assert.notNull(executionContext, "The ExecutionContext must not be null.");

        DBObject dbObject = new BasicDBObject(executionIdKey, executionId);
        for (Map.Entry<String, Object> entry : executionContext.entrySet()) {
            dbObject.put(entry.getKey(), entry.getValue());
        }
        getCollection().update(new BasicDBObject(executionIdKey, executionId), dbObject, true, true);
    }

    private ExecutionContext getExecutionContext(String executionIdKey, Long executionId) {
        Assert.notNull(executionId, "ExecutionId must not be null.");
        DBObject result = getCollection().findOne(new BasicDBObject(executionIdKey, executionId));
        ExecutionContext executionContext = new ExecutionContext();
        if (result != null) {
            result.removeField(executionIdKey);
            for (String key : result.keySet()) {
                executionContext.put(key, result.get(key));
            }
        }
        return executionContext;
    }

    protected DBCollection getCollection() {
        return db.getCollection(ExecutionContext.class.getSimpleName());
    }

}