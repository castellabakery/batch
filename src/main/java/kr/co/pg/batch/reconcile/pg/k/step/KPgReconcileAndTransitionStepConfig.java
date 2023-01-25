package kr.co.pg.batch.reconcile.pg.k.step;

import kr.co.pg.batch.reconcile.common.domain.PaymentMainDto;
import kr.co.pg.batch.reconcile.common.domain.entity.PaymentAddinfo;
import kr.co.pg.batch.reconcile.common.domain.entity.PaymentDetail;
import kr.co.pg.batch.reconcile.common.domain.entity.PaymentMain;
import kr.co.pg.batch.reconcile.common.step.listener.CommonStepLoggingListener;
import kr.co.pg.batch.reconcile.common.step.listener.KReconcileAndTransitionPaymentMainStepExecutionListener;
import kr.co.pg.batch.reconcile.pg.k.KPgReconcileBatchConfig;
import kr.co.pg.batch.reconcile.pg.k.domain.PgCheckCustidSet;
import kr.co.pg.batch.reconcile.pg.k.domain.entity.PgReconcileCompleted;
import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
import kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper;
import kr.co.pg.batch.reconcile.pg.k.step.listener.KPgReconcileAndTransitionStepExecutionListener;
import kr.co.pg.util.DateFormatUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisPagingItemReaderBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
public class KPgReconcileAndTransitionStepConfig {
    private final String RECONCILE_PG_INSERT_PAYMENT_MAIN_STEP = KPgReconcileBatchConfig.PREFIX + "InsertPaymentMainStep";
    private final String RECONCILE_PG_UPDATE_PAYMENT_MAIN_STEP = KPgReconcileBatchConfig.PREFIX + "UpdatePaymentMainStep";
    private final String RECONCILE_PG_INSERT_PAYMENT_ADDINFO_STEP = KPgReconcileBatchConfig.PREFIX + "InsertPaymentAddinfoStep";
    private final String RECONCILE_PG_INSERT_PAYMENT_DETAIL_STEP = KPgReconcileBatchConfig.PREFIX + "InsertPaymentDetailStep";
    private final String RECONCILE_PG_CHECK_CUSTID_STEP = KPgReconcileBatchConfig.PREFIX + "checkCustidStep";
    private final String RECONCILE_PG_CHECK_ORDERNO_STEP = KPgReconcileBatchConfig.PREFIX + "checkOrderNoStep";
    private final String RECONCILE_PG_STEP = KPgReconcileBatchConfig.PREFIX + "Step";


    private final String GET_QUERY_ID = "kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper.getReconcilePg";
    private final String INSERT_QUERY_ID = "kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper.insertReconcilePg";

    private final String PAYMENT_MAIN_GET_QUERY_ID = "kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper.getReconcilePgForPaymentMain";
    private final String PAYMENT_MAIN_INSERT_QUERY_ID = "kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper.insertPaymentMain";

    private final String PAYMENT_ADDINFO_GET_QUERY_ID = "kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper.getReconcilePgForPaymentAddinfo";
    private final String PAYMENT_ADDINFO_INSERT_QUERY_ID = "kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper.insertPaymentAddinfo";

    private final String PAYMENT_DETAIL_GET_QUERY_ID = "kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper.getReconcilePgForPaymentDetail";
    private final String PAYMENT_DETAIL_INSERT_QUERY_ID = "kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper.insertPaymentDetail";

    private final String RECONCILE_PG_ORDERNO_CHECK_QUERY_ID = "kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper.getReconcilePgForUpdatePaymentMain";
    private final String RECONCILE_PG_INSERT_UPDATE_QUERY_ID = "kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper.insertUpdatePaymentMain";
    private final int CHUNK_SIZE = 100;

    private final StepBuilderFactory stepBuilderFactory;
    private final SqlSessionFactory sqlSessionFactory;
    private final KPgReconcileJobParameters kPgReconcileJobParameters;
    private final KPgReconcileAndTransitionStepExecutionListener kPgReconcileAndTransitionStepExecutionListener;
    private final ReconcilePgMapper reconcilePgMapper;
    private final KReconcileAndTransitionPaymentMainStepExecutionListener kReconcileAndTransitionPaymentMainStepExecutionListener;

    private final CommonStepLoggingListener commonStepLoggingListener;

    private final DateTimeFormatter STANDARD_DATE_FORMATTER = DateFormatUtils.DASH_DATE;
    private final String PAYMENT_ROUTE_PG = "PG";

    private final Integer dbConfiguration;

    public KPgReconcileAndTransitionStepConfig(StepBuilderFactory stepBuilderFactory, SqlSessionFactory sqlSessionFactory, KPgReconcileJobParameters kPgReconcileJobParameters, KPgReconcileAndTransitionStepExecutionListener kPgReconcileAndTransitionStepExecutionListener, ReconcilePgMapper reconcilePgMapper
            , KReconcileAndTransitionPaymentMainStepExecutionListener kReconcileAndTransitionPaymentMainStepExecutionListener
            , CommonStepLoggingListener commonStepLoggingListener
            , @Value("${db-configuration}") Integer dbConfiguration
    ) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.sqlSessionFactory = sqlSessionFactory;
        this.kPgReconcileJobParameters = kPgReconcileJobParameters;
        this.kPgReconcileAndTransitionStepExecutionListener = kPgReconcileAndTransitionStepExecutionListener;
        this.reconcilePgMapper = reconcilePgMapper;
        this.kReconcileAndTransitionPaymentMainStepExecutionListener = kReconcileAndTransitionPaymentMainStepExecutionListener;
        this.commonStepLoggingListener = commonStepLoggingListener;
        this.dbConfiguration = dbConfiguration;
    }

    @Bean
    @JobScope
    public Step reconcilePgPaymentMainForCheckOrderNoStep() {
        return stepBuilderFactory.get(RECONCILE_PG_CHECK_ORDERNO_STEP)
                .listener(kReconcileAndTransitionPaymentMainStepExecutionListener)
                .listener(commonStepLoggingListener)
                .<PaymentMain, PaymentMain>chunk(CHUNK_SIZE)
                .reader(reconcilePgPaymentMainForCheckOrderNoReader(kPgReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .writer(reconcilePgPaymentMainForCheckOrderNoWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PaymentMain> reconcilePgPaymentMainForCheckOrderNoReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);

        return new MyBatisPagingItemReaderBuilder<PaymentMain>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(RECONCILE_PG_ORDERNO_CHECK_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private MyBatisBatchItemWriter<PaymentMain> reconcilePgPaymentMainForCheckOrderNoWriter() {
        return new MyBatisBatchItemWriterBuilder<PaymentMain>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(RECONCILE_PG_INSERT_UPDATE_QUERY_ID)
                .build();
    }

    @Bean
    public Step reconcilePgPaymentMainUpdateStep() {
        return stepBuilderFactory.get(RECONCILE_PG_UPDATE_PAYMENT_MAIN_STEP)
                .tasklet(reconcilePgCompleteBeforeTasklet())
                .build();
    }

    @Bean
    @JobScope
    public Step reconcilePgPaymentMainStep() {
        return stepBuilderFactory.get(RECONCILE_PG_INSERT_PAYMENT_MAIN_STEP)
                .listener(commonStepLoggingListener)
                .<PaymentMain, PaymentMainDto>chunk(CHUNK_SIZE)
                .reader(reconcilePgPaymentMainReader(kPgReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .processor(entityToDtoProcessor())
                .writer(reconcilePgPaymentMainWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PaymentMain> reconcilePgPaymentMainReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);

        return new MyBatisPagingItemReaderBuilder<PaymentMain>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(PAYMENT_MAIN_GET_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private ItemProcessor<PaymentMain, PaymentMainDto> entityToDtoProcessor() {
        return item -> new PaymentMainDto(item, dbConfiguration);
    }

    private MyBatisBatchItemWriter<PaymentMainDto> reconcilePgPaymentMainWriter() {
        return new MyBatisBatchItemWriterBuilder<PaymentMainDto>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(PAYMENT_MAIN_INSERT_QUERY_ID)
                .build();
    }

    @Bean
    @JobScope
    public Step reconcilePgPaymentAddinfoStep() {
        return stepBuilderFactory.get(RECONCILE_PG_INSERT_PAYMENT_ADDINFO_STEP)
                .listener(commonStepLoggingListener)
                .<PaymentAddinfo, PaymentAddinfo>chunk(CHUNK_SIZE)
                .reader(reconcilePgPaymentAddinfoReader(kPgReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .writer(reconcilePgPaymentAddinfoWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PaymentAddinfo> reconcilePgPaymentAddinfoReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);
        return new MyBatisPagingItemReaderBuilder<PaymentAddinfo>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(PAYMENT_ADDINFO_GET_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private MyBatisBatchItemWriter<PaymentAddinfo> reconcilePgPaymentAddinfoWriter() {
        return new MyBatisBatchItemWriterBuilder<PaymentAddinfo>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(PAYMENT_ADDINFO_INSERT_QUERY_ID)
                .build();
    }

    @Bean
    @JobScope
    public Step reconcilePgPaymentDetailStep() {
        return stepBuilderFactory.get(RECONCILE_PG_INSERT_PAYMENT_DETAIL_STEP)
                .listener(commonStepLoggingListener)
                .<PaymentDetail, PaymentDetail>chunk(CHUNK_SIZE)
                .reader(reconcilePgPaymentDetailReader(kPgReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .writer(reconcilePgPaymentDetailWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PaymentDetail> reconcilePgPaymentDetailReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);
        return new MyBatisPagingItemReaderBuilder<PaymentDetail>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(PAYMENT_DETAIL_GET_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private MyBatisBatchItemWriter<PaymentDetail> reconcilePgPaymentDetailWriter() {
        return new MyBatisBatchItemWriterBuilder<PaymentDetail>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(PAYMENT_DETAIL_INSERT_QUERY_ID)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet reconcilePgCompleteBeforeTasklet() {
        return (contribution, chunkContext) -> {
            String standardDate = kPgReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER);
            try {
                reconcilePgMapper.updatePaymentMainReconcileInsertCompletedForTid(standardDate);
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to update payment main insert completed for tid from reconcile - reason: ")
                        .append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }
            try {
                reconcilePgMapper.updatePaymentMainReconcileInsertCompletedForOrgTid(standardDate);
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to update payment main insert completed for orgtid from reconcile - reason: ")
                        .append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }
            try {
                reconcilePgMapper.updatePaymentMainReconcileInsertCompleted(standardDate);
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to update payment main insert completed from reconcile - reason: ")
                        .append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    @JobScope
    public Step reconcilePgStep() {
        return stepBuilderFactory.get(RECONCILE_PG_STEP)
                .listener(kPgReconcileAndTransitionStepExecutionListener)
                .listener(commonStepLoggingListener)
                .<PgReconcileCompleted, PgReconcileCompleted>chunk(CHUNK_SIZE)
                .reader(reconcilePgReader(kPgReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .writer(reconcilePgWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PgReconcileCompleted> reconcilePgReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);
        return new MyBatisPagingItemReaderBuilder<PgReconcileCompleted>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(GET_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private MyBatisBatchItemWriter<PgReconcileCompleted> reconcilePgWriter() {
        return new MyBatisBatchItemWriterBuilder<PgReconcileCompleted>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(INSERT_QUERY_ID)
                .build();
    }

    @Bean
    @JobScope
    public Step checkCustidPgStep() {
        return stepBuilderFactory.get(RECONCILE_PG_CHECK_CUSTID_STEP)
                .tasklet(checkCustidPgTasklet())
                .build();
    }

    @Bean
    @StepScope
    public Tasklet checkCustidPgTasklet() {
        return (contribution, chunkContext) -> {
            String standardDate = kPgReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER);
            List<PgCheckCustidSet> custidList = reconcilePgMapper.selectPgCustidListForCheck(standardDate);
            for (PgCheckCustidSet custidSet : custidList) {
                if (StringUtils.equals(custidSet.getResult(), "1")) {
                    log.error("Failed to check custid exist from pg reconcile - [{}]", custidSet);
                    throw new Exception("NoCustidException - storeNo: " + custidSet.getStoreNo());
                } else {
                    log.info("custidMap - [{}]", custidSet);
                }
            }
            return RepeatStatus.FINISHED;
        };
    }
}
