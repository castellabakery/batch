package kr.co.pg.batch.reconcile.van.k.tasklet;

import kr.co.pg.batch.reconcile.van.k.job.parameters.KVanReconcileJobParameters;
import kr.co.pg.batch.reconcile.van.k.service.KVanReconcileDeleteService;
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
public class KVanReconcileFailOverTasklet implements Tasklet {


    private final KVanReconcileJobParameters kVanReconcileJobParameters;

    private final KVanReconcileDeleteService kVanReconcileDeleteService;

    @Autowired
    public KVanReconcileFailOverTasklet(KVanReconcileJobParameters kVanReconcileJobParameters
            , KVanReconcileDeleteService kVanReconcileDeleteService
    ) {
        this.kVanReconcileJobParameters = kVanReconcileJobParameters;
        this.kVanReconcileDeleteService = kVanReconcileDeleteService;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        try {
            kVanReconcileDeleteService.deleteAll(kVanReconcileJobParameters.getStandardDate());
        } catch (Exception e) {
            log.error("Failed to delete all k van reconcile data - standardDate: {}", kVanReconcileJobParameters.getStandardDate());
            throw e;
        }

        return RepeatStatus.FINISHED;
    }


}
