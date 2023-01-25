package kr.co.pg.batch.reconcile.pg.k.tasklet;

import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
import kr.co.pg.batch.reconcile.pg.k.service.KPgReconcileDeleteService;
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
public class KPgReconcileFailOverTasklet implements Tasklet {


    private final KPgReconcileJobParameters kPgReconcileJobParameters;

    private final KPgReconcileDeleteService kPgReconcileDeleteService;

    @Autowired
    public KPgReconcileFailOverTasklet(KPgReconcileJobParameters kPgReconcileJobParameters
            , KPgReconcileDeleteService kPgReconcileDeleteService) {
        this.kPgReconcileJobParameters = kPgReconcileJobParameters;
        this.kPgReconcileDeleteService = kPgReconcileDeleteService;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        try {
            kPgReconcileDeleteService.deleteAll(kPgReconcileJobParameters.getStandardDate());
        } catch (Exception e) {
            log.error("Failed to delete all k pg reconcile data - standardDate: {}", kPgReconcileJobParameters.getStandardDate());
            throw e;
        }

        return RepeatStatus.FINISHED;
    }


}
