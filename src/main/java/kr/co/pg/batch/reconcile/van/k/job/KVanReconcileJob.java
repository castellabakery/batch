package kr.co.pg.batch.reconcile.van.k.job;

import kr.co.pg.batch.DefaultJob;
import kr.co.pg.batch.DefaultJobParameters;
import kr.co.pg.batch.reconcile.common.service.BatchFailService;
import kr.co.pg.batch.reconcile.van.k.job.parameters.KVanReconcileJobParameters;
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
public class KVanReconcileJob extends DefaultJob {

    private Job kVanReconcileBatchJob;
    private StandardJobLauncher jobLauncher;
    private final BatchFailService batchFailService;

    private final String JOB_NAME = "kVanReconcileBatchJob";


    @Autowired
    public KVanReconcileJob(StandardJobLauncher jobLauncher, @Qualifier(JOB_NAME) Job kVanReconcileBatchJob
            , BatchFailService batchFailService) {
        this.jobLauncher = jobLauncher;
        this.kVanReconcileBatchJob = kVanReconcileBatchJob;
        this.batchFailService = batchFailService;
    }

    @Override
    public JobExecutionVo executeExpandedJob(SchedulerJobParameters schedulerJobParameters) throws Exception {
        JobParameters jobParameters = schedulerJobParameters.getJobParameters();
        return jobLauncher.execute(this.kVanReconcileBatchJob, jobParameters);
    }

    @Override
    public SchedulerJobParameters scheduledJobParameters() {
        return new SchedulerJobParameters(new KVanReconcileJobParameters(DEFAULT_JOB_EXECUTOR_NAME));
    }

    @Override
    public DefaultJobParameters forcedJobParameters(Map<String, String> jobParameterMap) throws Exception {
        return convertMapToDefaultParameter(jobParameterMap, new KVanReconcileJobParameters());
    }

    @Override
    public void failProcess(String errorMessage, String exceptionMessage) {
        batchFailService.execute(JOB_NAME, scheduledJobParameters(), errorMessage, exceptionMessage);
    }

}
