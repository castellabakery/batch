package kr.co.pg.batch.reconcile.common.step.listener;

import kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KReconcileAndTransitionPaymentMainStepExecutionListener extends StepExecutionListenerSupport {

    private final PaymentMainMapper paymentMainMapper;
    private final Integer dbConfiguration;

    public KReconcileAndTransitionPaymentMainStepExecutionListener(PaymentMainMapper paymentMainMapper, @Value("${db-configuration}") Integer dbConfiguration) {
        this.paymentMainMapper = paymentMainMapper;
        this.dbConfiguration = dbConfiguration;
    }

    public void beforeStep(StepExecution stepExecution){
        try {
            paymentMainMapper.initiateTid(dbConfiguration);
        } catch(Exception e) {
            log.warn("Failed to initiate tid - [{}]", e.getMessage());
        }
    }
}
