package org.springframework.batch.mongo.dao;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.mongodb.BasicDBObjectBuilder.start;
import static org.springframework.batch.mongo.config.ApplicationConfiguration.DOT_ESCAPE_STRING;
import static org.springframework.batch.mongo.config.ApplicationConfiguration.DOT_STRING;

/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 15, 2010
 */
@Repository
public class MongoJobInstanceDao extends AbstractMongoDao implements JobInstanceDao {

    public static final String JOB_NAME_KEY = "jobName";
    static final String JOB_INSTANCE_ID_KEY = "jobInstanceId";
    protected static final String JOB_KEY_KEY = "jobKey";
    protected static final String JOB_PARAMETERS_KEY = "jobParameters";

    @PostConstruct
    public void init() {
        getCollection().ensureIndex(jobInstanceIdObj(1L));
    }

    @Override
    public JobInstance createJobInstance(String jobName, final JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        Assert.state(getJobInstance(jobName, jobParameters) == null,
                "JobInstance must not already exist");

        Long jobId = getNextId(JobInstance.class.getSimpleName());

        JobInstance jobInstance = new JobInstance(jobId, jobParameters, jobName);

        jobInstance.incrementVersion();

        Map<String, JobParameter> jobParams = jobParameters.getParameters();
        Map<String, Object> paramMap = Maps.newHashMapWithExpectedSize(jobParams.size());
        for (Map.Entry<String, JobParameter> entry : jobParams.entrySet()) {
            paramMap.put(entry.getKey().replaceAll(DOT_STRING, DOT_ESCAPE_STRING), entry.getValue().getValue());
        }
        getCollection().save(start()
                .add(JOB_INSTANCE_ID_KEY, jobId)
                .add(JOB_NAME_KEY, jobName)
                .add(JOB_KEY_KEY, createJobKey(jobParameters))
                .add(VERSION_KEY, jobInstance.getVersion())
                .add(JOB_PARAMETERS_KEY, new BasicDBObject(paramMap)).get());
        return jobInstance;
    }

    @Override
    public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        String jobKey = createJobKey(jobParameters);

        return mapJobInstance(getCollection().findOne(start()
                .add(JOB_NAME_KEY, jobName)
                .add(JOB_KEY_KEY, jobKey).get()), jobParameters);
    }

    @Override
    public JobInstance getJobInstance(Long instanceId) {
        return mapJobInstance(getCollection().findOne(jobInstanceIdObj(instanceId)));
    }

    @Override
    public JobInstance getJobInstance(JobExecution jobExecution) {
        DBObject instanceId = db.getCollection(JobExecution.class.getSimpleName()).findOne(MongoJobExecutionDao.jobExecutionIdObj(jobExecution.getId()), jobInstanceIdObj(1L));
        removeSystemFields(instanceId);
        return mapJobInstance(getCollection().findOne(instanceId));
    }

    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        return mapJobInstances(getCollection().find(new BasicDBObject(JOB_NAME_KEY, jobName)).sort(jobInstanceIdObj(-1L)).skip(start).limit(count));
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public List<String> getJobNames() {
        List results = getCollection().distinct(JOB_NAME_KEY);
        Collections.sort(results);
        return results;
    }

    protected String createJobKey(JobParameters jobParameters) {

        Map<String, JobParameter> props = jobParameters.getParameters();
        StringBuilder stringBuilder = new StringBuilder();
        List<String> keys = new ArrayList<String>(props.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            stringBuilder.append(key).append("=").append(props.get(key).toString()).append(";");
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "MD5 algorithm not available.  Fatal (should be in the JDK).");
        }

        try {
            byte[] bytes = digest.digest(stringBuilder.toString().getBytes(
                    "UTF-8"));
            return String.format("%032x", new BigInteger(1, bytes));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(
                    "UTF-8 encoding not available.  Fatal (should be in the JDK).");
        }
    }

    @Override
    protected DBCollection getCollection() {
        return db.getCollection(JobInstance.class.getSimpleName());
    }

    private List<JobInstance> mapJobInstances(DBCursor dbCursor) {
        List<JobInstance> results = Lists.newArrayList();
        while (dbCursor.hasNext()) {
            results.add(mapJobInstance(dbCursor.next()));
        }
        return results;
    }

    private JobInstance mapJobInstance(DBObject dbObject) {
        return mapJobInstance(dbObject, null);
    }

    private JobInstance mapJobInstance(@Nullable DBObject dbObject, JobParameters jobParameters) {
        JobInstance jobInstance = null;
        if (dbObject != null) {
            Long id = (Long) dbObject.get(JOB_INSTANCE_ID_KEY);
            if (jobParameters == null) {
                jobParameters = getJobParameters(id);
            }
            jobInstance = new JobInstance(id, jobParameters, (String) dbObject.get(JOB_NAME_KEY)); // should always be at version=0 because they never get updated
            jobInstance.incrementVersion();
        }
        return jobInstance;
    }

    @SuppressWarnings({"unchecked"})
    private JobParameters getJobParameters(Long jobInstanceId) {
        final Map<String, ?> jobParamsMap = (Map<String, Object>) getCollection().findOne(new BasicDBObject(jobInstanceIdObj(jobInstanceId))).get(JOB_PARAMETERS_KEY);

        Map<String, JobParameter> map = Maps.newHashMapWithExpectedSize(jobParamsMap.size());
        for (Map.Entry<String, ?> entry : jobParamsMap.entrySet()) {
            Object param = entry.getValue();
            String key = entry.getKey().replaceAll(DOT_ESCAPE_STRING, DOT_STRING);
            if (param instanceof String) {
                map.put(key, new JobParameter((String) param));
            } else if (param instanceof Long) {
                map.put(key, new JobParameter((Long) param));
            } else if (param instanceof Double) {
                map.put(key, new JobParameter((Double) param));
            } else if (param instanceof Date) {
                map.put(key, new JobParameter((Date) param));
            } else {
                map.put(key, null);
            }
        }
        return new JobParameters(map);
    }

    static BasicDBObject jobInstanceIdObj(Long id) {
        return new BasicDBObject(MongoJobInstanceDao.JOB_INSTANCE_ID_KEY, id);
    }

}
