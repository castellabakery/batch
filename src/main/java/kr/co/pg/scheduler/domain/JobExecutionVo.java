package kr.co.pg.scheduler.domain;

import lombok.Getter;
import lombok.ToString;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Getter
@ToString
public class JobExecutionVo {

    private final long jobExecutionId;

    private final long jobInstanceId;

    private final LocalDateTime startDateTime;
    private final LocalDateTime endDateTime;

    private final String jobBatchName;

    private final Map<String, String> jobParameterMap;

    private final BatchStatus batchStatus;
    private final ExitStatus exitStatus;

    private final List<Throwable> stepExceptionList;

    public JobExecutionVo(long jobExecutionId, long jobInstanceId, LocalDateTime startDateTime, LocalDateTime endDateTime
            , String jobBatchName, Map<String, String> jobParameterMap, BatchStatus batchStatus, ExitStatus exitStatus
            , List<Throwable> stepExceptionList
    ) {
        this.jobExecutionId = jobExecutionId;
        this.jobInstanceId = jobInstanceId;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.jobBatchName = jobBatchName;
        this.jobParameterMap = jobParameterMap;
        this.batchStatus = batchStatus;
        this.exitStatus = exitStatus;
        this.stepExceptionList = stepExceptionList;
    }

}
