package org.springframework.batch.mongo.dao;

import org.junit.runner.RunWith;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * Created by IntelliJ IDEA.
 *
 * @author Baruch S.
 * @since Apr 22, 2010
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:application-config.xml"})
public class MongoStepExecutionDaoTests extends AbstractStepExecutionDaoTests {

    @Autowired
    private StepExecutionDao stepExecutionDao;

    @Autowired
    private JobRepository jobRepository;

    @Override
    protected StepExecutionDao getStepExecutionDao() {
        return stepExecutionDao;
    }

    @Override
    protected JobRepository getJobRepository() {
        return jobRepository;
    }
}
