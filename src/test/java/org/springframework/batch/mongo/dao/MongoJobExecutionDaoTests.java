package org.springframework.batch.mongo.dao;

import org.junit.runner.RunWith;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;

/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 22, 2010
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:application-config.xml"})
public class MongoJobExecutionDaoTests extends AbstractJobExecutionDaoTests {

    @Inject
    private JobExecutionDao jobExecutionDao;
    @Inject
    private JobInstanceDao jobInstanceDao;

    @Override
    protected JobExecutionDao getJobExecutionDao() {
        return jobExecutionDao;
    }

    @Override
    protected JobInstanceDao getJobInstanceDao() {
        return jobInstanceDao;
    }
}