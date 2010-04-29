package org.springframework.batch.mongo.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.math.BigInteger;
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
    protected static final String TYPE_SUFFIX = "_TYPE";
    private static final Logger LOG = LoggerFactory.getLogger(MongoExecutionContextDao.class);

    @PostConstruct
    public void init() {
        getCollection().ensureIndex(BasicDBObjectBuilder.start().add(STEP_EXECUTION_ID_KEY, 1).add(JOB_EXECUTION_ID_KEY, 1).get());
    }

    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        return getExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId());
    }

    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        return getExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId());
    }

    public void saveExecutionContext(JobExecution jobExecution) {
        saveOrUpdateExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId(), jobExecution.getExecutionContext());
    }

    public void saveExecutionContext(StepExecution stepExecution) {
        saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
    }

    public void updateExecutionContext(JobExecution jobExecution) {
        saveOrUpdateExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId(), jobExecution.getExecutionContext());
    }

    public void updateExecutionContext(StepExecution stepExecution) {
        saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
    }

    private void saveOrUpdateExecutionContext(String executionIdKey, Long executionId, ExecutionContext executionContext) {
        Assert.notNull(executionId, "ExecutionId must not be null.");
        Assert.notNull(executionContext, "The ExecutionContext must not be null.");

        DBObject dbObject = new BasicDBObject(executionIdKey, executionId);
        for (Map.Entry<String, Object> entry : executionContext.entrySet()) {
            Object value = entry.getValue();
            String key = entry.getKey();
            dbObject.put(key, value);
            if (value instanceof BigDecimal || value instanceof BigInteger) {
                dbObject.put(key + TYPE_SUFFIX, value.getClass().getName());
            }
        }
        getCollection().update(new BasicDBObject(executionIdKey, executionId), dbObject, true, false);
    }

    @SuppressWarnings({"unchecked"})
    private ExecutionContext getExecutionContext(String executionIdKey, Long executionId) {
        Assert.notNull(executionId, "ExecutionId must not be null.");
        DBObject result = getCollection().findOne(new BasicDBObject(executionIdKey, executionId));
        ExecutionContext executionContext = new ExecutionContext();
        if (result != null) {
            result.removeField(executionIdKey);
            removeSystemFields(result);
            for (String key : result.keySet()) {
                Object value = result.get(key);
                String type = (String) result.get(key + TYPE_SUFFIX);
                if (type != null && Number.class.isAssignableFrom(value.getClass())) {
                    try {
                        value = NumberUtils.convertNumberToTargetClass((Number) value, (Class<? extends Number>) Class.forName(type));
                    } catch (Exception e) {
                        LOG.warn("Failed to convert {} to {}", key, type);
                    }
                }
                executionContext.put(key, value);
            }
        }
        return executionContext;
    }

    protected DBCollection getCollection() {
        return db.getCollection(ExecutionContext.class.getSimpleName());
    }

}