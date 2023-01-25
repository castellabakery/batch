package kr.co.pg.batch.count.monthly.job;

import kr.co.pg.batch.DefaultJob;
import kr.co.pg.batch.DefaultJobParameters;
import kr.co.pg.batch.count.monthly.job.parameters.CountMonthlyJobParameters;
import kr.co.pg.scheduler.domain.JobExecutionVo;
import kr.co.pg.scheduler.domain.SchedulerJobParameters;
import kr.co.pg.scheduler.launcher.StandardJobLauncher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class CountMonthlyJob extends DefaultJob {

    private Job countMonthlyBatchJob;
    private StandardJobLauncher jobLauncher;


    @Autowired
    public CountMonthlyJob(StandardJobLauncher jobLauncher, @Qualifier("countMonthlyBatchJob") Job countMonthlyBatchJob) {
        this.jobLauncher = jobLauncher;
        this.countMonthlyBatchJob = countMonthlyBatchJob;
    }

    @Override
    public JobExecutionVo executeExpandedJob(SchedulerJobParameters schedulerJobParameters) throws Exception {
        JobParameters jobParameters = schedulerJobParameters.getJobParameters();
        return jobLauncher.execute(this.countMonthlyBatchJob, jobParameters);
    }

    @Override
    public SchedulerJobParameters scheduledJobParameters() {
        return new SchedulerJobParameters(new CountMonthlyJobParameters(DEFAULT_JOB_EXECUTOR_NAME));
    }

    @Override
    public DefaultJobParameters forcedJobParameters(Map<String, String> jobParameterMap) throws Exception {
        return convertMapToDefaultParameter(jobParameterMap, new CountMonthlyJobParameters());
    }

    @Override
    public void failProcess(String errorMessage, String exceptionMessage) {
    }

}
