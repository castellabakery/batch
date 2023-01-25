package kr.co.pg.batch;

import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.springframework.batch.core.Job;

/**
 * 해당 인터페이스를 상속받아 구현하는 BatchConfig에서 Job의 이름은 [기능 + 'BatchJob'] 으로 한다.
 * (Spring Batch Job의 명명 규칙으로 정의)
 */
public interface DefaultBatchConfig {

    Job job() throws Exception;

    JobDetail jobDetail();

    Trigger trigger();

    String getBatchJobName();

    String getBatchJobGroupName();

    String getTriggerName();

    String getTriggerGroupName();

    String getCronSchedule();

}
