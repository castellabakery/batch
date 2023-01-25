package kr.co.pg.batch.reconcile.pg.k;

import kr.co.pg.batch.DefaultBatchConfig;
import kr.co.pg.batch.reconcile.pg.k.job.KPgReconcileJob;
import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
import kr.co.pg.batch.reconcile.pg.k.step.*;
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
public class KPgReconcileBatchConfig implements DefaultBatchConfig {

    public final static String PREFIX = "kPgReconcile";

    private final static String BATCH_JOB_NAME = PREFIX + "BatchJob";
    private final static String BATCH_JOB_FLOW_NAME = PREFIX + "BatchJobFlow";
    private final static String BATCH_JOB_DETAIL_NAME = PREFIX + "JobDetail";

    private final static String BATCH_JOB_GROUP_NAME = PREFIX + "JobGroup";

    private static final String TRIGGER_NAME = PREFIX + "Trigger";

    private static final String TRIGGER_GROUP_NAME = PREFIX + "TriggerGroup";

    private static final String JOB_PARAMETERS = PREFIX + "jobParameters";

    private static final String CRON_SCHEDULE = "0 30 3 * * ?";

    private final JobBuilderFactory jobBuilderFactory;
    private final KPgReconcileFilesDownloadStepConfig kPgReconcileFilesDownloadStepConfig;
    private final KPgReconcileFilesReadStepConfig kPgReconcileFilesReadStepConfig;
    private final KPgReconcileOriginParsingStepConfig kPgReconcileOriginParsingStepConfig;
    private final KPgWebMigrationStepConfig kPgWebMigrationStepConfig;
    private final KPgReconcileAndTransitionStepConfig kPgReconcileAndTransitionStepConfig;

    private final KPgReconcileFailOverStepConfig kPgReconcileFailOverStepConfig;

    public KPgReconcileBatchConfig(JobBuilderFactory jobBuilderFactory, KPgReconcileFilesDownloadStepConfig kPgReconcileFilesDownloadStepConfig, KPgReconcileFilesReadStepConfig kPgReconcileFilesReadStepConfig, KPgReconcileOriginParsingStepConfig kPgReconcileOriginParsingStepConfig, KPgWebMigrationStepConfig kPgWebMigrationStepConfig, KPgReconcileAndTransitionStepConfig kPgReconcileAndTransitionStepConfig
            , KPgReconcileFailOverStepConfig kPgReconcileFailOverStepConfig) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.kPgReconcileFilesDownloadStepConfig = kPgReconcileFilesDownloadStepConfig;
        this.kPgReconcileFilesReadStepConfig = kPgReconcileFilesReadStepConfig;
        this.kPgReconcileOriginParsingStepConfig = kPgReconcileOriginParsingStepConfig;
        this.kPgWebMigrationStepConfig = kPgWebMigrationStepConfig;
        this.kPgReconcileAndTransitionStepConfig = kPgReconcileAndTransitionStepConfig;
        this.kPgReconcileFailOverStepConfig = kPgReconcileFailOverStepConfig;
    }

    private final String EXIT_STATUS_COMPLETE = "COMPLETED";
    private final boolean IS_ALLOWED_FAIL_OVER = true;

    @Override
    @Bean(name = BATCH_JOB_NAME)
    public Job job() throws Exception {

        org.springframework.batch.core.job.builder.JobBuilder jobBuilder = jobBuilderFactory.get(BATCH_JOB_NAME);

        // 거래대사(Lst) 파일 다운로드 step
        FlowBuilder flowBuilder = new FlowBuilder(BATCH_JOB_FLOW_NAME).start(kPgReconcileFilesDownloadStepConfig.downloadLst())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileFilesReadStepConfig.readLst())
                .from(kPgReconcileFilesDownloadStepConfig.downloadLst())
                .on("*").fail();

        // 거래대사(Lst) file read step
        FlowBuilder.TransitionBuilder tbReadLst = flowBuilder.from(kPgReconcileFilesReadStepConfig.readLst())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileOriginParsingStepConfig.parseOriginLst())
                .from(kPgReconcileFilesReadStepConfig.readLst())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReadLst.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReadLst.fail();
        }

        // 거래대사(Lst) parsing step
        FlowBuilder.TransitionBuilder tbParseOriginLst = flowBuilder.from(kPgReconcileOriginParsingStepConfig.parseOriginLst())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainForCheckOrderNoStep())
                .from(kPgReconcileOriginParsingStepConfig.parseOriginLst())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbParseOriginLst.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbParseOriginLst.fail();
        }

        // check orderno step
        FlowBuilder.TransitionBuilder tbCheckOrderno = flowBuilder.start(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainForCheckOrderNoStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileAndTransitionStepConfig.checkCustidPgStep())
                .from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainForCheckOrderNoStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbCheckOrderno.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbCheckOrderno.fail();
        }

        // check custid exist step
        FlowBuilder.TransitionBuilder tbCheckCustid = flowBuilder.from(kPgReconcileAndTransitionStepConfig.checkCustidPgStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainStep())
                .from(kPgReconcileAndTransitionStepConfig.checkCustidPgStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbCheckCustid.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbCheckCustid.fail();
        }

        // 대사 payment main step
        FlowBuilder.TransitionBuilder tbReconcilePaymentMain = flowBuilder.from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentAddinfoStep())
                .from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcilePaymentMain.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcilePaymentMain.fail();
        }

        // 대사 payment addinfo step
        FlowBuilder.TransitionBuilder tbReconcilePaymentAddinfo = flowBuilder.from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentAddinfoStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentDetailStep())
                .from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentAddinfoStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcilePaymentAddinfo.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcilePaymentAddinfo.fail();
        }

        // 대사 payment detail step
        FlowBuilder.TransitionBuilder tbReconcilePaymentDetail = flowBuilder.from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentDetailStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainUpdateStep())
                .from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentDetailStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcilePaymentDetail.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcilePaymentDetail.fail();
        }

        // 대사 payment main update step
        FlowBuilder.TransitionBuilder tbReconcilePaymentMainUpdate = flowBuilder.from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainUpdateStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgReconcileAndTransitionStepConfig.reconcilePgStep())
                .from(kPgReconcileAndTransitionStepConfig.reconcilePgPaymentMainUpdateStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcilePaymentMainUpdate.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcilePaymentMainUpdate.fail();
        }

        // 대사 코어 처리 step
        FlowBuilder.TransitionBuilder tbReconcile = flowBuilder.from(kPgReconcileAndTransitionStepConfig.reconcilePgStep())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgWebMigrationStepConfig.migrateApproveWebData())
                .from(kPgReconcileAndTransitionStepConfig.reconcilePgStep())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbReconcile.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbReconcile.fail();
        }

        // 승인 거래 web migration step
        FlowBuilder.TransitionBuilder tbMigrationApproveWebData = flowBuilder.from(kPgWebMigrationStepConfig.migrateApproveWebData())
                .on(EXIT_STATUS_COMPLETE)
                .to(kPgWebMigrationStepConfig.migrateCancelWebData())
                .from(kPgWebMigrationStepConfig.migrateApproveWebData())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbMigrationApproveWebData.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbMigrationApproveWebData.fail();
        }

        // 취소 거래 web migration step
        FlowBuilder.TransitionBuilder tbMigrationCancelWebData = flowBuilder.from(kPgWebMigrationStepConfig.migrateCancelWebData())
                .on(EXIT_STATUS_COMPLETE)
                .end() // job 정상 종료 처리
                .from(kPgWebMigrationStepConfig.migrateCancelWebData())
                .on("*");

        if (IS_ALLOWED_FAIL_OVER) {
            flowBuilder = tbMigrationCancelWebData.to(kPgReconcileFailOverStepConfig.deleteAll()).on("*").fail();
        } else {
            flowBuilder = tbMigrationCancelWebData.fail();
        }

        Flow flow = (Flow) flowBuilder.build();
        return jobBuilder.start(flow).end().build();

    }

    @Override
    @Bean(name = BATCH_JOB_DETAIL_NAME)
    public JobDetail jobDetail() {
        JobDetail jobDetail = JobBuilder.newJob(KPgReconcileJob.class)
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
    public KPgReconcileJobParameters jobParameters(@Value("#{jobParameters[standardDate]}") String standardDate,
                                                   @Value("#{jobParameters[adminId]}") String adminId) {
        return new KPgReconcileJobParameters(standardDate, adminId);
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
