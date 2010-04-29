package org.springframework.batch.mongo.config;

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.javaconfig.util.ConfigurationSupport;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.net.UnknownHostException;

/**
 * Created by IntelliJ IDEA.
 *
 * @author JBaruch
 * @since 19-Apr-2010
 */
@Configuration
public class ApplicationConfiguration {

    @Value("${app.db.name}")
    private String appDbName;

    @Value("${batch.db.name}")
    private String batchDbName;

    @Value("${step.thread.core.pool.size}")
    private int corePoolSize;

    @Value("${step.thread.max.pool.size}")
    private int maxPoolSize;


    @Autowired
    private ConfigurationSupport configurationSupport;

    @Autowired
    private ExecutionContextDao executionContextDao;

    @Autowired
    private JobExecutionDao jobExecutionDao;

    @Autowired
    private JobInstanceDao jobInstanceDao;

    @Autowired
    private StepExecutionDao stepExecutionDao;

    public static final String DOT_ESCAPE_STRING = "\\{dot\\}";
    public static final String DOT_STRING = "\\.";


    @Bean
    public SimpleJobOperator jobOperator() {
        SimpleJobOperator jobOperator = new SimpleJobOperator();
        jobOperator.setJobLauncher(jobLauncher());
        jobOperator.setJobExplorer(jobExplorer());
        jobOperator.setJobRepository(jobRepository());
        jobOperator.setJobRegistry(jobRegistry());
        return jobOperator;
    }

    @Bean
    public JobExplorer jobExplorer() {
        return new SimpleJobExplorer(jobInstanceDao, jobExecutionDao, stepExecutionDao, executionContextDao);
    }

    @Bean
    public JobRegistry jobRegistry() {
        return new MapJobRegistry();
    }

    @Bean
    JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor() {
        JobRegistryBeanPostProcessor postProcessor = new JobRegistryBeanPostProcessor();
        postProcessor.setJobRegistry(jobRegistry());
        return postProcessor;
    }

    @Bean
    public SimpleJobLauncher jobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository());
        return jobLauncher;
    }

    @Bean
    public JobRepository jobRepository() {
        return new SimpleJobRepository(jobInstanceDao, jobExecutionDao, stepExecutionDao, executionContextDao);
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(corePoolSize);
        taskExecutor.setMaxPoolSize(maxPoolSize);
        return taskExecutor;
    }

    @Bean
    public JobParametersIncrementer jobParametersIncrementer() {
        return new RunIdIncrementer();
    }

    @Bean
    @Database(Database.Purpose.APPLICATION)
    public DB applicationDb() throws UnknownHostException {
        return mongo().getDB(appDbName);
    }

    @Bean
    @Database(Database.Purpose.BATCH)
    public DB batchDb() throws UnknownHostException {
        return mongo().getDB(batchDbName);
    }

    @Bean
    public Mongo mongo() throws UnknownHostException {
        return new Mongo();
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }


}
