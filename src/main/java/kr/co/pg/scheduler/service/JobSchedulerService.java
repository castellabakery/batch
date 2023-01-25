package kr.co.pg.scheduler.service;

import kr.co.pg.api.JobParametersDetail;
import kr.co.pg.api.domain.dto.JobParametersDetailDto;
import kr.co.pg.batch.DefaultBatchConfig;
import kr.co.pg.batch.DefaultJob;
import kr.co.pg.batch.DefaultJobParameters;
import kr.co.pg.batch.reconcile.common.service.BatchFailService;
import kr.co.pg.scheduler.domain.JobExecutionVo;
import kr.co.pg.scheduler.domain.JobSchedulerVo;
import kr.co.pg.scheduler.domain.SchedulerJobParameters;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class JobSchedulerService {

    private final ApplicationContext applicationContext;
    private final SchedulerHelper schedulerHandler;

    private final BatchFailService batchFailService;

    private final String[] exceptJobs = {"kPgReconcileTrigger"};

    @Autowired
    public JobSchedulerService(ApplicationContext applicationContext, SchedulerHelper schedulerHandler
            , BatchFailService batchFailService) {
        this.applicationContext = applicationContext;
        this.schedulerHandler = schedulerHandler;
        this.batchFailService = batchFailService;
    }

    public List<JobSchedulerVo> init(boolean isStoppedWhenFail) throws Exception {
        List<JobSchedulerVo> initiatedScheduler = new ArrayList<>();

        String[] beanNames = applicationContext.getBeanNamesForType(DefaultBatchConfig.class);
        List<String> triggerNames = new ArrayList<>();
        for (String beanName : beanNames) {

            // 본래 로직
            DefaultBatchConfig config = (DefaultBatchConfig) applicationContext.getBean(beanName);
            JobSchedulerVo vo = new JobSchedulerVo(config);

            boolean isRegistered = this.registerJobScheduler(vo);
            if (isRegistered) {
                initiatedScheduler.add(vo);
            } else {
                if (isStoppedWhenFail) {
                    log.error("Failed to register - {}", vo);
                    throw new Exception("Failed to register");
                }
            }
            triggerNames.add(vo.getTriggerName());
        }

        for (String job : exceptJobs) {
            boolean isExist = false;
            for (String triggerName : triggerNames) {
                if (triggerName.equals(job)) {
                    isExist = true;
                }
            }
            if (isExist) {
                schedulerHandler.unScheduler(job);
            } else {
                log.error("Failed to except batch jobName - " + job);
                throw new Exception("Failed to except batch jobName - " + job);
            }
        }

        return initiatedScheduler;
    }

    public boolean registerJobScheduler(JobSchedulerVo vo) {
        log.info("register scheduler - {}", vo);

        try {
            schedulerHandler.registerScheduler(vo.getJobDetail(), vo.getTrigger());
        } catch (Exception e) {
            log.error("Failed to register batch job - scheduler: {}, reason: {}", vo, e.getMessage());
            return false;
        }

        return true;
    }

    public JobExecutionVo executeJob(String jobName, Map<String, String> jobParameterMap) throws Exception {

        DefaultJob job = getDefaultJob(jobName);
        DefaultJobParameters jobParameters = getDefaultJobParameters(job, jobParameterMap);
        SchedulerJobParameters schedulerJobParameters = new SchedulerJobParameters(true, jobParameters);

        try {
            jobParameters.validate();
        } catch (Exception e) {
            log.error("Failed to validate Default Job parameter in forced batch - reason: {}", e.getMessage());
            batchFailService.execute(jobName, schedulerJobParameters, "Failed to validate job parameter in forced batch", e.getMessage());
            throw e;
        }

        JobExecutionVo jobExecutionVo;
        try {
            jobExecutionVo = job.executeExpandedJob(schedulerJobParameters);
        } catch (Exception e) {
            log.error("Failed to execute job in forced batch - {}", schedulerJobParameters);
            batchFailService.execute(jobName, schedulerJobParameters, "Failed to execute job in forced batch", e.getMessage());
            throw e;
        }

        BatchStatus batchStatus = jobExecutionVo.getBatchStatus();
        ExitStatus exitStatus = jobExecutionVo.getExitStatus();

        if (batchStatus != BatchStatus.COMPLETED || !exitStatus.equals(ExitStatus.COMPLETED)) {
            log.error("Invalid Batch Job Status in forced batch - job name: {}, batch status: {}, exit status: {}", jobExecutionVo.getJobBatchName(), batchStatus, exitStatus);

            List<Throwable> throwableList = jobExecutionVo.getStepExceptionList();
            StringBuilder exceptionMessages = new StringBuilder("batch status: ").append(batchStatus).append(", exit status: ").append(exitStatus).append("\n");
            if (throwableList != null) {
                for (Throwable t : throwableList) {
                    exceptionMessages.append(t.getMessage()).append("\n");
                }
            }

            batchFailService.execute(jobName, schedulerJobParameters, "Invalid Batch Job Status in forced batch", exceptionMessages.toString());
            throw new JobExecutionException("Invalid Batch Job Status in forced batch");
        }

        return jobExecutionVo;
    }

    public List<JobParametersDetailDto> getJobParametersDetail(String jobName) throws Exception {

        DefaultJob job = getDefaultJob(jobName);
        DefaultJobParameters jobParameters = job.scheduledJobParameters().getDefaultJobParameters();

        if (!(jobParameters instanceof JobParametersDetail)) {
            log.error("No Job Parameters Detail - jobName: {}", jobName);
            throw new Exception("No Job Parameters Detail");
        }

        return ((JobParametersDetail) jobParameters).getJobParametersDetailList();
    }

    private DefaultJob getDefaultJob(String jobName) throws NullPointerException {
        String[] beanNames = applicationContext.getBeanNamesForType(DefaultJob.class);
        if (beanNames == null || beanNames.length < 1) {
            log.error("No bean with DefaultJob name.");
            throw new NullPointerException("No bean with DefaultJob name");
        }

        DefaultJob job = (DefaultJob) applicationContext.getBean(jobName);
        if (job == null) {
            log.error("No bean with job name: {}", jobName);
            throw new NullPointerException("No bean with name");
        }

        return job;
    }

    private DefaultJobParameters getDefaultJobParameters(DefaultJob job, Map<String, String> jobParameterMap) throws Exception {

        DefaultJobParameters jobParameters;
        try {
            jobParameters = job.forcedJobParameters(jobParameterMap);
        } catch (Exception e) {
            log.error("Failed to get default job parameter");
            throw e;
        }

        return jobParameters;
    }

}
