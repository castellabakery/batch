package kr.co.pg.batch;

import kr.co.pg.scheduler.domain.JobExecutionVo;
import kr.co.pg.scheduler.domain.SchedulerJobParameters;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.util.Assert;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 해당 추상클래스를 상속받아 구현하는 Job의 이름은 [기능 + 'Job'] 으로 한다.
 * (QuartzJob의 명명 규칙으로 정의)
 */
@Slf4j
public abstract class DefaultJob extends QuartzJobBean {

    public static final String DEFAULT_JOB_EXECUTOR_NAME = "SYSTEM";

    public abstract JobExecutionVo executeExpandedJob(SchedulerJobParameters schedulerJobParameters) throws Exception;

    public abstract SchedulerJobParameters scheduledJobParameters();

    public abstract DefaultJobParameters forcedJobParameters(Map<String, String> jobParameterMap) throws Exception;

    public abstract void failProcess(String errorMessage, String exceptionMessage);

    @Override
    final protected void executeInternal(JobExecutionContext context) throws JobExecutionException {

        SchedulerJobParameters schedulerJobParameters = scheduledJobParameters();

        try {
            validate(schedulerJobParameters);
        } catch (Exception e) {
            log.error("Failed to validate job parameter in regular batch - reason: {}", e.getMessage());
            failProcess("Failed to validate job parameter in regular batch", e.getMessage());
            throw new JobExecutionException(e.getMessage());
        }

        JobExecutionVo jobExecutionVo;
        try {
            jobExecutionVo = executeExpandedJob(schedulerJobParameters);
        } catch (Exception e) {
            log.error("Failed to execute job in regular batch - request job parameter: {}, reason: {}", schedulerJobParameters, e.getMessage());
            failProcess("Failed to execute job in regular batch", e.getMessage());
            throw new JobExecutionException("Failed to execute regular job");
        }

        BatchStatus batchStatus = jobExecutionVo.getBatchStatus();
        ExitStatus exitStatus = jobExecutionVo.getExitStatus();

        if (batchStatus != BatchStatus.COMPLETED || !exitStatus.equals(ExitStatus.COMPLETED)) {
            log.error("Invalid Batch Job Status in regular batch - job name: {}, batch status: {}, exit status: {}", jobExecutionVo.getJobBatchName(), batchStatus, exitStatus);
            List<Throwable> throwableList = jobExecutionVo.getStepExceptionList();

            StringBuilder exceptionMessages = new StringBuilder("batch status: ").append(batchStatus).append(", exit status: ").append(exitStatus).append("\n");
            if (throwableList != null) {
                for (Throwable t : throwableList) {
                    exceptionMessages.append(t.getMessage()).append("\n");
                }
            }

            failProcess("Invalid Batch Job Status in regular batch", exceptionMessages.toString());
            throw new JobExecutionException("Invalid Batch Job Status in regular batch");
        }

        log.info("success execution job - job execution: {}", jobExecutionVo);
    }

    private void validate(SchedulerJobParameters scheduledJobParameters) throws Exception {
        Assert.notNull(scheduledJobParameters, "scheduledJobParameters must not be null.");

        DefaultJobParameters defaultJobParameters = scheduledJobParameters.getDefaultJobParameters();
        try {
            defaultJobParameters.validate();
        } catch (Exception e) {
            log.error("Failed to validate Default Job parameter - reason: {}", e.getMessage());
            throw e;
        }

    }

    final static protected DefaultJobParameters convertMapToDefaultParameter(Map<String, String> map, DefaultJobParameters defaultJobParameters) throws Exception {
        Iterator it = map.keySet().iterator();

        final String SETTER_PREFIX = "set";

        int invokedCount = 0;
        while (it.hasNext()) {
            String key = (String) it.next();
            String methodString = new StringBuilder(SETTER_PREFIX)
                    .append(key.substring(0, 1).toUpperCase())
                    .append(key.substring(1)).toString();

            Method[] methods = defaultJobParameters.getClass().getDeclaredMethods();
            for (Method m : methods) {
                if (methodString.equals(m.getName())) {
                    try {
                        m.invoke(defaultJobParameters, map.get(key));
                        invokedCount++;
                        continue;
                    } catch (Exception e) {
                        String errorMessage = new StringBuilder("Failed to invoke set jobParameter - reason: ")
                                .append(e.getCause().getMessage()).toString();
                        log.error(errorMessage);
                        throw new Exception(errorMessage);
                    }
                }
            }
        }

        if (map.size() != invokedCount) {
            log.error("Failed to called method successfully - required method count: {}, invoked count: {}", map.size(), invokedCount);
            throw new Exception("Failed to called method successfully - required method count: " + map.size() + ", invoked count: " + invokedCount);
        }

        return defaultJobParameters;
    }

}
