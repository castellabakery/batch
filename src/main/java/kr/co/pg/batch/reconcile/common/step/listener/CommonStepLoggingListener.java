package kr.co.pg.batch.reconcile.common.step.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CommonStepLoggingListener extends StepExecutionListenerSupport {

	public CommonStepLoggingListener() {

	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {

		String name = stepExecution.getStepName();
		int readCount = stepExecution.getReadCount();
		int readSkipCount = stepExecution.getReadSkipCount();
		int writeCount = stepExecution.getWriteCount();
		int writeSkipCount = stepExecution.getWriteSkipCount();
		int filterCount = stepExecution.getFilterCount();
		int processSkipCount = stepExecution.getProcessSkipCount();
		int commitCount = stepExecution.getCommitCount();


		log.info("Finished {} - readCount: {}, readSkipCount: {}, writeCount: {}, writeSkipCount: {}, fileterCount: {}, processSkipCount: {}, commitCount: {}"
				, name, readCount, readSkipCount, writeCount, writeSkipCount
				, filterCount, processSkipCount, commitCount);

		return ExitStatus.COMPLETED;
	}
}
