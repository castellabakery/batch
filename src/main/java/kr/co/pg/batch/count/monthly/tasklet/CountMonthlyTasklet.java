package kr.co.pg.batch.count.monthly.tasklet;

import kr.co.pg.batch.count.monthly.domain.CountMonthlyRequestDto;
import kr.co.pg.batch.count.monthly.job.parameters.CountMonthlyJobParameters;
import kr.co.pg.batch.count.monthly.mapper.CountMonthlyMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@StepScope
public class CountMonthlyTasklet implements Tasklet {

    private final CountMonthlyMapper countMonthlyMapper;
    private final CountMonthlyJobParameters countMonthlyJobParameters;


    @Autowired
    public CountMonthlyTasklet(CountMonthlyMapper countMonthlyMapper, CountMonthlyJobParameters countMonthlyJobParameters) {
        this.countMonthlyMapper = countMonthlyMapper;
        this.countMonthlyJobParameters = countMonthlyJobParameters;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        CountMonthlyRequestDto request = new CountMonthlyRequestDto(countMonthlyJobParameters.getBiDate(), countMonthlyJobParameters.getRegId());
        log.info("Start Monthly Count -  request: {}", request);

        try {
            countMonthlyMapper.callProcedureCountMonthly(request);
        } catch (Exception e) {
            log.error("Failed to execute count monthly procedure - request: {}, reason: {}", request, e.getMessage());
            throw e;
        }

        log.info("Finish Monthly Count -  request: {}", request);

        return RepeatStatus.FINISHED;
    }
}
