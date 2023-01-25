package kr.co.pg.batch.reconcile.pg.k.job;

import kr.co.pg.batch.DefaultJob;
import kr.co.pg.batch.DefaultJobParameters;
import kr.co.pg.batch.reconcile.common.service.BatchFailService;
import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
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
public class KPgReconcileJob extends DefaultJob {

    private Job kPgReconcileBatchJob;
    private StandardJobLauncher jobLauncher;
    private final BatchFailService batchFailService;

    private final String JOB_NAME = "kPgReconcileBatchJob";


    @Autowired
    public KPgReconcileJob(StandardJobLauncher jobLauncher, @Qualifier(JOB_NAME) Job kPgReconcileBatchJob
            , BatchFailService batchFailService) {
        this.jobLauncher = jobLauncher;
        this.kPgReconcileBatchJob = kPgReconcileBatchJob;
        this.batchFailService = batchFailService;
    }

    @Override
    public JobExecutionVo executeExpandedJob(SchedulerJobParameters schedulerJobParameters) throws Exception {
        JobParameters jobParameters = schedulerJobParameters.getJobParameters();
        return jobLauncher.execute(this.kPgReconcileBatchJob, jobParameters);
    }

    @Override
    public SchedulerJobParameters scheduledJobParameters() {
        return new SchedulerJobParameters(new KPgReconcileJobParameters(DEFAULT_JOB_EXECUTOR_NAME));
    }

    @Override
    public DefaultJobParameters forcedJobParameters(Map<String, String> jobParameterMap) throws Exception {
        return convertMapToDefaultParameter(jobParameterMap, new KPgReconcileJobParameters());
    }

    @Override
    public void failProcess(String errorMessage, String exceptionMessage) {
        batchFailService.execute(JOB_NAME, scheduledJobParameters(), errorMessage, exceptionMessage);
    }

}
