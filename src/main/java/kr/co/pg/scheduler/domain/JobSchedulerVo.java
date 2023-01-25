package kr.co.pg.scheduler.domain;

import kr.co.pg.batch.DefaultBatchConfig;
import lombok.Getter;
import lombok.ToString;
import org.quartz.JobDetail;
import org.quartz.Trigger;

@Getter
@ToString
public class JobSchedulerVo {
    private String jobName;
    private String triggerName;
    private String cronSchedule;
    private String jobGroup;
    private String triggerGroup;

    private JobDetail jobDetail;
    private Trigger trigger;

    public JobSchedulerVo(String jobName, String triggerName, String cronSchedule, String jobGroup, String triggerGroup
            , JobDetail jobDetail, Trigger trigger) {
        this.jobName = jobName;
        this.triggerName = triggerName;
        this.cronSchedule = cronSchedule;
        this.jobGroup = jobGroup;
        this.triggerGroup = triggerGroup;
        this.jobDetail = jobDetail;
        this.trigger = trigger;

    }

    public JobSchedulerVo(DefaultBatchConfig config) {
        this (config.getBatchJobName(), config.getTriggerName(), config.getCronSchedule(), config.getBatchJobGroupName(), config.getTriggerGroupName(), config.jobDetail(), config.trigger());
    }
}
