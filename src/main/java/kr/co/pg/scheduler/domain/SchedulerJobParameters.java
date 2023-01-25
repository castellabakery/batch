package kr.co.pg.scheduler.domain;

import kr.co.pg.batch.DefaultJobParameters;
import lombok.Getter;
import lombok.ToString;
import org.springframework.batch.core.JobParameters;

@Getter
@ToString
public class SchedulerJobParameters {

    private static final boolean IS_EXTERNAL_PARAMETER = false;

    private final boolean isExternalJobParameter;
    private final DefaultJobParameters defaultJobParameters;

    public SchedulerJobParameters(DefaultJobParameters jobParameters) {
        this(IS_EXTERNAL_PARAMETER, jobParameters);
    }

    public SchedulerJobParameters(boolean isExternalJobParameter, DefaultJobParameters defaultJobParameters) {
        this.isExternalJobParameter = isExternalJobParameter;
        this.defaultJobParameters = defaultJobParameters;
    }

    public JobParameters getJobParameters() {
       return defaultJobParameters.toJobParameter();
    }

}
