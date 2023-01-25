package kr.co.pg.batch.reconcile.van.k.step.listener;

import kr.co.pg.batch.reconcile.van.k.job.parameters.KVanReconcileJobParameters;
import kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KVanReconcileAndTransitionStepExecutionListener extends StepExecutionListenerSupport {

    private final KVanReconcileJobParameters kVanReconcileJobParameters;
    private final ReconcileVanMapper reconcileVanMapper;

    public KVanReconcileAndTransitionStepExecutionListener(KVanReconcileJobParameters kVanReconcileJobParameters, ReconcileVanMapper reconcileVanMapper) {
        this.kVanReconcileJobParameters = kVanReconcileJobParameters;
        this.reconcileVanMapper = reconcileVanMapper;
    }

    public ExitStatus afterStep(StepExecution stepExecution) {
        String standardDate = kVanReconcileJobParameters.getStandardDate().format(KVanReconcileJobParameters.DATE_FORMATTER);
        try {
            reconcileVanMapper.insertVanReconcileHistoryData(standardDate);
        } catch (Exception e) {
            log.error("Failed to insert reconcile history data - [{}]", e.getMessage());
        }
        return ExitStatus.COMPLETED;
    }

}
