package kr.co.pg.batch.reconcile.van.k;

import kr.co.pg.batch.DefaultBatchConfig;
import kr.co.pg.batch.reconcile.van.k.job.KVanReconcileJob;
import kr.co.pg.batch.reconcile.van.k.job.parameters.KVanReconcileJobParameters;
import kr.co.pg.batch.reconcile.van.k.step.KVanReconcileAndTransitionStepConfig;
import kr.co.pg.batch.reconcile.van.k.step.KVanReconcileFailOverStepConfig;
import kr.co.pg.batch.reconcile.van.k.step.KVanWebMigrationStepConfig;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KVanReconcileBatchConfig implements DefaultBatchConfig {

    public final static String PREFIX = "kVanReconcile";

    private final static String BATCH_JOB_NAME = PREFIX + "BatchJob";
    private final static String BATCH_JOB_FLOW_NAME = PREFIX + "BatchJobFlow";
    private final static String BATCH_JOB_DETAIL_NAME = PREFIX + "JobDetail";

    private final static String BATCH_JOB_GROUP_NAME = PREFIX + "JobGroup";

    private static final String TRIGGER_NAME = PREFIX + "Trigger";

    private static final String TRIGGER_GROUP_NAME = PREFIX + "TriggerGroup";

    private static final String JOB_PARAMETERS = PREFIX + "jobParameters";

    private static final String CRON_SCHEDULE = "0 30 4 * * ?";

    private final JobBuilderFactory jobBuilderFactory;


    private final KVanWebMigrationStepConfig kVanWebMigrationStepConfig;
    private final KVanReconcileAndTransitionStepConfig kVanReconcileAndTransitionStepConfig;

    private final KVanReconcileFailOverStepConfig kVanReconcileFailOverStepConfig;

    public KVanReconcileBatchConfig(JobBuilderFactory jobBuilderFactory
            , KVanWebMigrationStepConfig kVanWebMigrationStepConfig
            , KVanReconcileAndTransitionStepConfig kVanReconcileAndTransitionStepConfig
            , KVanReconcileFailOverStepConfig kVanReconcileFailOverStepConfig) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.kVanWebMigrationStepConfig = kVanWebMigrationStepConfig;
        this.kVanReconcileAndTransitionStepConfig = kVanReconcileAndTransitionStepConfig;
        this.kVanReconcileFailOverStepConfig = kVanReconcileFailOverStepConfig;
    }

    private final String EXIT_STATUS_COMPLETE = "COMPLETED";
    private final boolean IS_ALLOWED_FAIL_OVER = true;

    @Override
    @Bean(name = BATCH_JOB_NAME)
    public Job job() throws Exception {

        org.springframework.batch.core.job.builder.JobBuilder jobBuilder = jobBuilderFactory.get(BATCH_JOB_NAME);

        FlowBuilder flowBuilder = new FlowBuilder(BATCH_JOB_FLOW_NAME);

        // check orderno step
        FlowBuilder.TransitionBuilder tbCheckOrderno = flowBuilder.start(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentMainForCheckOrderNoStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kVanReconcileAndTransitionStepConfig.checkCustidVanStep())
                .from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentMainForCheckOrderNoStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbCheckOrderno.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbCheckOrderno.fail();
        }

        // check custid exist step
        FlowBuilder.TransitionBuilder tbCheckCustid = flowBuilder.from(kVanReconcileAndTransitionStepConfig.checkCustidVanStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentMainStep())
                .from(kVanReconcileAndTransitionStepConfig.checkCustidVanStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbCheckCustid.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbCheckCustid.fail();
        }

        // 대사 payment main step
        FlowBuilder.TransitionBuilder tbReconcilePaymentMain = flowBuilder.from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentMainStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentAddinfoStep())
                .from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentMainStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcilePaymentMain.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcilePaymentMain.fail();
        }

        // 대사 payment addinfo step
        FlowBuilder.TransitionBuilder tbReconcilePaymentAddinfo = flowBuilder.from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentAddinfoStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentDetailStep())
                .from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentAddinfoStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcilePaymentAddinfo.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcilePaymentAddinfo.fail();
        }

        // 대사 payment detail step
        FlowBuilder.TransitionBuilder tbReconcilePaymentDetail = flowBuilder.from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentDetailStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentMainUpdateStep())
                .from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentDetailStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcilePaymentDetail.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcilePaymentDetail.fail();
        }

//         대사 payment main update step
        FlowBuilder.TransitionBuilder tbReconcilePaymentMainUpdate = flowBuilder.from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentMainUpdateStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kVanReconcileAndTransitionStepConfig.reconcileVanStep())
                .from(kVanReconcileAndTransitionStepConfig.reconcileVanPaymentMainUpdateStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcilePaymentMainUpdate.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcilePaymentMainUpdate.fail();
        }

        // 대사 코어 처리 step
        FlowBuilder.TransitionBuilder tbReconcile = flowBuilder.from(kVanReconcileAndTransitionStepConfig.reconcileVanStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kVanWebMigrationStepConfig.migrateApproveWebData())
                .from(kVanReconcileAndTransitionStepConfig.reconcileVanStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcile.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcile.fail();
        }

        // 승인 거래 web migration step
        FlowBuilder.TransitionBuilder tbMigrationApproveWebData = flowBuilder.from(kVanWebMigrationStepConfig.migrateApproveWebData())
                .on(EXIT_STATUS_COMPLETE)
                .to(kVanWebMigrationStepConfig.migrateCancelWebData())
                .from(kVanWebMigrationStepConfig.migrateApproveWebData())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbMigrationApproveWebData.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbMigrationApproveWebData.fail();
        }

        // 취소 거래 web migration step
        FlowBuilder.TransitionBuilder tbMigrationCancelWebData = flowBuilder.from(kVanWebMigrationStepConfig.migrateCancelWebData())
                .on(EXIT_STATUS_COMPLETE)
                .end() // job 정상 종료 처리
                .from(kVanWebMigrationStepConfig.migrateCancelWebData())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbMigrationCancelWebData.to(kVanReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbMigrationCancelWebData.fail();
        }

        Flow flow = (Flow) flowBuilder.build();
        return jobBuilder.start(flow).end().build();
    }

    @Override
    @Bean(name = BATCH_JOB_DETAIL_NAME)
    public JobDetail jobDetail() {
        JobDetail jobDetail = JobBuilder.newJob(KVanReconcileJob.class)
                .storeDurably()
                .withIdentity(new JobKey(BATCH_JOB_NAME))
                .build();
        return jobDetail;
    }

    @Override
    @Bean(name = TRIGGER_NAME)
    public Trigger trigger() {
        Trigger trigger = TriggerBuilder.newTrigger()
                .forJob(jobDetail())
                .withIdentity(new TriggerKey(TRIGGER_NAME))
                .withSchedule(CronScheduleBuilder.cronSchedule(CRON_SCHEDULE))
                .build();
        return trigger;
    }

    @Bean(JOB_PARAMETERS)
    @JobScope
    public KVanReconcileJobParameters jobParameters(@Value("#{jobParameters[standardDate]}") String standardDate,
                                                    @Value("#{jobParameters[adminId]}") String adminId) {
        return new KVanReconcileJobParameters(standardDate, adminId);
    }

    @Override
    public String getTriggerName() {
        return TRIGGER_NAME;
    }

    @Override
    public String getBatchJobName() {
        return BATCH_JOB_NAME;
    }

    @Override
    public String getCronSchedule() {
        return CRON_SCHEDULE;
    }

    @Override
    public String getBatchJobGroupName() {
        return BATCH_JOB_GROUP_NAME;
    }

    @Override
    public String getTriggerGroupName() {
        return TRIGGER_GROUP_NAME;
    }

}
