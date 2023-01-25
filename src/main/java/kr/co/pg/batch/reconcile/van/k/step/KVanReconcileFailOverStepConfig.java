package kr.co.pg.batch.reconcile.van.k.step;

import kr.co.pg.batch.reconcile.van.k.KVanReconcileBatchConfig;
import kr.co.pg.batch.reconcile.van.k.tasklet.KVanReconcileFailOverTasklet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KVanReconcileFailOverStepConfig {

    private final String STEP_DELETE_ALL_DATA = KVanReconcileBatchConfig.PREFIX + "FailOverStep";

    private final StepBuilderFactory stepBuilderFactory;

    private final KVanReconcileFailOverTasklet kVanReconcileFailOverTasklet;

    @Autowired
    public KVanReconcileFailOverStepConfig(StepBuilderFactory stepBuilderFactory
            , KVanReconcileFailOverTasklet kVanReconcileFailOverTasklet) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.kVanReconcileFailOverTasklet = kVanReconcileFailOverTasklet;
    }

    @Bean(name = STEP_DELETE_ALL_DATA)
    @JobScope
    public Step deleteAll() {
        Step step = stepBuilderFactory.get(STEP_DELETE_ALL_DATA)
                .tasklet(kVanReconcileFailOverTasklet)
                .build();
        return step;
    }

}
