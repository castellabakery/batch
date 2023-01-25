package kr.co.pg.batch.count.monthly;

import kr.co.pg.batch.DefaultBatchConfig;
import kr.co.pg.batch.count.monthly.job.CountMonthlyJob;
import kr.co.pg.batch.count.monthly.job.parameters.CountMonthlyJobParameters;
import kr.co.pg.batch.count.monthly.tasklet.CountMonthlyTasklet;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class CountMonthlyBatchConfig implements DefaultBatchConfig {

    private final static String PREFIX = "countMonthly";

    private final static String BATCH_JOB_NAME = PREFIX + "BatchJob";
    private final static String BATCH_JOB_DETAIL_NAME = PREFIX + "JobDetail";

    private final static String BATCH_JOB_GROUP_NAME = PREFIX + "JobGroup";

    private static final String STEP_NAME = PREFIX + "Step";

    private static final String TRIGGER_NAME = PREFIX + "Trigger";

    private static final String TRIGGER_GROUP_NAME = PREFIX + "TriggerGroup";

    private static final String JOB_PARAMETERS = PREFIX + "jobParameters";

    private static final String CRON_SCHEDULE = "0 30 5 * * ?";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    private final CountMonthlyTasklet countMonthlyTasklet;

    @Autowired
    public CountMonthlyBatchConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, CountMonthlyTasklet countMonthlyTasklet) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.countMonthlyTasklet = countMonthlyTasklet;
    }

    @Bean(name = BATCH_JOB_NAME)
    @Override
    public Job job() {
        Job job = jobBuilderFactory.get(BATCH_JOB_NAME)
                .start(countMonthlyStep())
                .build();
        return job;
    }

    @Bean(name = STEP_NAME)
    @JobScope
    public Step countMonthlyStep() {
        Step step = stepBuilderFactory.get(STEP_NAME)
                .tasklet(tasklet())
                .build();
        return step;
    }

    private Tasklet tasklet() {
        return countMonthlyTasklet;
    }

    @Override
    @Bean(name = BATCH_JOB_DETAIL_NAME)
    public JobDetail jobDetail() {
        JobDetail jobDetail = JobBuilder.newJob(CountMonthlyJob.class)
                .storeDurably()
                .withIdentity(new JobKey(BATCH_JOB_NAME))
                .build();
        return jobDetail;
    }

    @Override
    @Bean(name = TRIGGER_NAME)
    public Trigger trigger() {
        Trigger trigger = TriggerBuilder.newTrigger()
                .forJob(jobDetail())
                .withIdentity(new TriggerKey(TRIGGER_NAME))
                .withSchedule(CronScheduleBuilder.cronSchedule(CRON_SCHEDULE))
                .build();
        return trigger;
    }

    @Bean(name = JOB_PARAMETERS)
    @JobScope
    public CountMonthlyJobParameters jobParameters(@Value("#{jobParameters[biDate]}") String biDate,
                                                   @Value("#{jobParameters[regId]}") String regId) {
        return new CountMonthlyJobParameters(biDate, regId);
    }

    @Override
    public String getTriggerName() {
        return TRIGGER_NAME;
    }

    @Override
    public String getBatchJobName() {
        return BATCH_JOB_NAME;
    }

    @Override
    public String getCronSchedule() {
        return CRON_SCHEDULE;
    }

    @Override
    public String getBatchJobGroupName() {
        return BATCH_JOB_GROUP_NAME;
    }

    @Override
    public String getTriggerGroupName() {
        return TRIGGER_GROUP_NAME;
    }
}
