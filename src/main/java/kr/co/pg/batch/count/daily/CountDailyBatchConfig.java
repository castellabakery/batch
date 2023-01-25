package kr.co.pg.batch.count.daily;

import kr.co.pg.batch.DefaultBatchConfig;
import kr.co.pg.batch.count.daily.job.CountDailyJob;
import kr.co.pg.batch.count.daily.job.parameters.CountDailyJobParameters;
import kr.co.pg.batch.count.daily.tasklet.CountDailyTasklet;
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
public class CountDailyBatchConfig implements DefaultBatchConfig {

    public final static String PREFIX = "countDaily";

    private final static String BATCH_JOB_NAME = PREFIX + "BatchJob";
    private final static String BATCH_JOB_DETAIL_NAME = PREFIX + "JobDetail";

    private final static String BATCH_JOB_GROUP_NAME = PREFIX + "JobGroup";

    private static final String STEP_NAME = PREFIX + "Step";

    private static final String TRIGGER_NAME = PREFIX + "Trigger";

    private static final String TRIGGER_GROUP_NAME = PREFIX + "TriggerGroup";

    private static final String JOB_PARAMETERS = PREFIX + "jobParameters";

    private static final String CRON_SCHEDULE = "0 0 5 * * ?";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    private final CountDailyTasklet countDailyTasklet;

    @Autowired
    public CountDailyBatchConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, CountDailyTasklet countDailyTasklet) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.countDailyTasklet = countDailyTasklet;
    }

    @Bean(name = BATCH_JOB_NAME)
    @Override
    public Job job() {
        Job job = jobBuilderFactory.get(BATCH_JOB_NAME)
                .start(countDailyStep())
                .build();
        return job;
    }

    @Bean(name = STEP_NAME)
    @JobScope
    public Step countDailyStep() {
        Step step = stepBuilderFactory.get(STEP_NAME)
                .tasklet(tasklet())
                .build();
        return step;
    }

    private Tasklet tasklet() {
        return countDailyTasklet;
    }

    @Override
    @Bean(name = BATCH_JOB_DETAIL_NAME)
    public JobDetail jobDetail() {
        JobDetail jobDetail = JobBuilder.newJob(CountDailyJob.class)
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
    public CountDailyJobParameters jobParameters(@Value("#{jobParameters[standardDate]}") String standardDate,
                                                 @Value("#{jobParameters[adminId]}") String adminId) {
        return new CountDailyJobParameters(standardDate, adminId);
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
