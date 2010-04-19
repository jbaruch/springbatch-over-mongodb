package org.springframework.batch.mongo.config;

import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.item.SimpleStepFactoryBean;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframwork.javaconfig.util.ConfigurationSupport;

import javax.inject.Inject;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 *
 * @author JBaruch
 * @since 19-Apr-2010
 */
@Configuration
public class JobConfiguration {

    @Value("${batch.start.limit}")
    private int startLimit;

    @Value("${batch.commit.interval}")
    private int commitInterval;


    @Inject
    private JobRepository jobRepository;

    @Inject
    private ItemReader<? extends String> itemReader;

    @Inject
    private ItemWriter<? super Object> itemWriter;

    @Inject
    private TaskExecutor taskExecutor;

    @Inject
    private ConfigurationSupport configurationSupport;

    @Inject
    private JobParametersIncrementer jobParametersIncrementer;

    @Bean
    public Step step() {
        SimpleStepFactoryBean<String, Object> stepFactoryBean = new SimpleStepFactoryBean<String, Object>();
        stepFactoryBean.setTransactionManager(new ResourcelessTransactionManager());
        stepFactoryBean.setJobRepository(jobRepository);
        stepFactoryBean.setStartLimit(startLimit);
        stepFactoryBean.setCommitInterval(commitInterval);
        stepFactoryBean.setItemReader(itemReader);
        stepFactoryBean.setItemWriter(itemWriter);
        stepFactoryBean.setTaskExecutor(taskExecutor);
        return configurationSupport.getObject(stepFactoryBean);
    }


    @Bean
    public AbstractJob job() {
        SimpleJob job = new SimpleJob();
        job.setJobParametersIncrementer(jobParametersIncrementer);
        job.setJobRepository(jobRepository);
        job.setSteps(Arrays.asList(step()));
        return job;
    }


}
