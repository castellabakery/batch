package kr.co.pg.scheduler.launcher;

import kr.co.pg.scheduler.domain.JobExecutionVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
public class StandardJobLauncher {

    private final JobLauncher jobLauncher;
    private final JobExplorer jobExplorer;

    @Autowired
    public StandardJobLauncher(JobLauncher jobLauncher, JobExplorer jobExplorer) {
        this.jobLauncher = jobLauncher;
        this.jobExplorer = jobExplorer;
    }

    public JobExecutionVo execute(Job job, JobParameters jobParameters) throws Exception {

        Set<JobExecution> jobExecutionSet = jobExplorer.findRunningJobExecutions(job.getName());
        if (jobExecutionSet != null && jobExecutionSet.size() > 0) {
            Iterator<JobExecution> iterator = jobExecutionSet.iterator();
            while (iterator.hasNext()) {
                JobExecution e = iterator.next();
                if (e.isRunning()) {
                    log.error("Job is already running - jobName: {}", job.getName());
                    return convertJobExecution(e);
                }
            }
        }

        JobParameters newJobParameters = new JobParametersBuilder()
                .addJobParameters(jobParameters)
                .addLong("timeMillis", System.currentTimeMillis())
                .toJobParameters();

        JobExecution jobExecution = jobLauncher.run(job, newJobParameters);

        return convertJobExecution(jobExecution);
    }

    private JobExecutionVo convertJobExecution(JobExecution jobExecution) {
        long jobExecutionId = jobExecution.getId();
        long jobInstanceId = jobExecution.getJobInstance().getInstanceId();
        LocalDateTime startDateTime = jobExecution.getStartTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalDateTime endDateTime = jobExecution.getEndTime().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();

        String jobName = jobExecution.getJobInstance().getJobName();

        Map<String, String> jobParameterMap = new HashMap<>();
        JobParameters jobParameters = jobExecution.getJobParameters();
        if (!jobParameters.isEmpty()) {
            for (String key : jobParameters.getParameters().keySet()) {
                jobParameterMap.put(key, String.valueOf(jobParameters.getParameters().get(key).getValue()));
            }
        }

        return new JobExecutionVo(jobExecutionId, jobInstanceId, startDateTime, endDateTime, jobName, jobParameterMap, jobExecution.getStatus(), jobExecution.getExitStatus(), jobExecution.getAllFailureExceptions());
    }
}
