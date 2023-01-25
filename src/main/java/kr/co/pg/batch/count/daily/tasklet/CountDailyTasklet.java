package kr.co.pg.batch.count.daily.tasklet;

import kr.co.pg.batch.count.daily.domain.CountDailyRequestDto;
import kr.co.pg.batch.count.daily.job.parameters.CountDailyJobParameters;
import kr.co.pg.batch.count.daily.mapper.CountDailyMapper;
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
public class CountDailyTasklet implements Tasklet {

    private final CountDailyMapper countDailyMapper;

    private final CountDailyJobParameters countDailyJobParameters;

    @Autowired
    public CountDailyTasklet(CountDailyMapper countDailyMapper, CountDailyJobParameters countDailyJobParameters) {
        this.countDailyMapper = countDailyMapper;
        this.countDailyJobParameters = countDailyJobParameters;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        CountDailyRequestDto request = new CountDailyRequestDto(countDailyJobParameters.getStandardDate(), countDailyJobParameters.getAdminId());
        log.info("Start Daily Count -  request: {}", request);

        try {
            countDailyMapper.callProcedureCountDaily(request);
        } catch (Exception e) {
            log.error("Failed to execute monthly count procedure - request: {}, reason: {}", request, e.getMessage());
            throw e;
        }

        log.info("Finish Daily Count -  request: {}", request);
        return RepeatStatus.FINISHED;
    }

}
