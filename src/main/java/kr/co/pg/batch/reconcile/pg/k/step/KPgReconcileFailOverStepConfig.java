package kr.co.pg.batch.reconcile.pg.k.step;

import kr.co.pg.batch.reconcile.pg.k.KPgReconcileBatchConfig;
import kr.co.pg.batch.reconcile.pg.k.tasklet.KPgReconcileFailOverTasklet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KPgReconcileFailOverStepConfig {

    private final String STEP_DELETE_ALL_DATA = KPgReconcileBatchConfig.PREFIX + "FailOverStep";

    private final StepBuilderFactory stepBuilderFactory;

    private final KPgReconcileFailOverTasklet kPgReconcileFailOverTasklet;

    @Autowired
    public KPgReconcileFailOverStepConfig(StepBuilderFactory stepBuilderFactory, KPgReconcileFailOverTasklet kPgReconcileFailOverTasklet) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.kPgReconcileFailOverTasklet = kPgReconcileFailOverTasklet;
    }

    @Bean(name = STEP_DELETE_ALL_DATA)
    @JobScope
    public Step deleteAll() {
        Step step = stepBuilderFactory.get(STEP_DELETE_ALL_DATA)
                .tasklet(kPgReconcileFailOverTasklet)
                .build();
        return step;
    }

}
