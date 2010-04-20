package org.springframework.batch.mongo.dao;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
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
import java.util.concurrent.ConcurrentMap;

import static com.mongodb.BasicDBObjectBuilder.start;

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

        Long jobId = getNexId(JobInstance.class.getSimpleName());

        JobInstance jobInstance = new JobInstance(jobId, jobParameters, jobName);

        jobInstance.incrementVersion();

        DBObject params = new BasicDBObject(new MapMaker().makeComputingMap(new Function<String, Object>() {
            @Override
            public Object apply(@Nullable String key) {
                return jobParameters.getParameters().get(key).getValue();
            }
        }));
        getCollection().save(start()
                .add(JOB_INSTANCE_ID_KEY, jobId)
                .add(JOB_NAME_KEY, jobName)
                .add(JOB_KEY_KEY, createJobKey(jobParameters))
                .add(VERSION_KEY, jobInstance.getVersion())
                .add(JOB_PARAMETERS_KEY, params).get());
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
        return mapJobInstance(getCollection().findOne(instanceId));
    }

    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        return mapJobInstances(getCollection().find(new BasicDBObject(JOB_NAME_KEY, jobName)).sort(jobInstanceIdObj(-1L)).skip(start - 1).limit(count));
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
        return db.getCollection(JobInstanceDao.class.getSimpleName());
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
        ConcurrentMap<String, JobParameter> map = new MapMaker().makeComputingMap(new Function<String, JobParameter>() {
            @Override
            public JobParameter apply(@Nullable String from) {
                Object param = jobParamsMap.get(from);
                if (param instanceof String) {
                    return new JobParameter((String) param);
                } else if (param instanceof Long) {
                    return new JobParameter((Long) param);
                } else if (param instanceof Double) {
                    return new JobParameter((Double) param);
                } else if (param instanceof Date) {
                    return new JobParameter((Date) param);
                } else {
                    return null;
                }
            }
        });
        return new JobParameters(map);
    }

    static BasicDBObject jobInstanceIdObj(Long id) {
        return new BasicDBObject(MongoJobInstanceDao.JOB_INSTANCE_ID_KEY, id);
    }

}
