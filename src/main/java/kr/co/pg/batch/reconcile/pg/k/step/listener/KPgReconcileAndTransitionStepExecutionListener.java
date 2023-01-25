package kr.co.pg.batch.reconcile.pg.k.step.listener;

import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
import kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KPgReconcileAndTransitionStepExecutionListener extends StepExecutionListenerSupport {

	private final KPgReconcileJobParameters kPgReconcileJobParameters;
	private final ReconcilePgMapper reconcilePgMapper;

	public KPgReconcileAndTransitionStepExecutionListener(KPgReconcileJobParameters kPgReconcileJobParameters, ReconcilePgMapper reconcilePgMapper) {
		this.kPgReconcileJobParameters = kPgReconcileJobParameters;
		this.reconcilePgMapper = reconcilePgMapper;
	}

	public ExitStatus afterStep(StepExecution stepExecution) {
		String standardDate = kPgReconcileJobParameters.getStandardDate().format(KPgReconcileJobParameters.DATE_FORMATTER);
		try {
			reconcilePgMapper.insertPgReconcileHistoryData(standardDate);
		} catch (Exception e) {
			log.error("Failed to insert reconcile history data - [{}]", e.getMessage());
		}
		return ExitStatus.COMPLETED;
	}

}
