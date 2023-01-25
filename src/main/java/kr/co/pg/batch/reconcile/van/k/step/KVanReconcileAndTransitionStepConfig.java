package kr.co.pg.batch.reconcile.van.k.step;

import kr.co.pg.batch.reconcile.common.domain.PaymentMainDto;
import kr.co.pg.batch.reconcile.common.domain.entity.PaymentAddinfo;
import kr.co.pg.batch.reconcile.common.domain.entity.PaymentDetail;
import kr.co.pg.batch.reconcile.common.domain.entity.PaymentMain;
import kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper;
import kr.co.pg.batch.reconcile.common.repository.PaymentDetailRepository;
import kr.co.pg.batch.reconcile.common.step.listener.CommonStepLoggingListener;
import kr.co.pg.batch.reconcile.common.step.listener.KReconcileAndTransitionPaymentMainStepExecutionListener;
import kr.co.pg.batch.reconcile.van.k.KVanReconcileBatchConfig;
import kr.co.pg.batch.reconcile.van.k.domain.VanCheckCustidSet;
import kr.co.pg.batch.reconcile.van.k.domain.entity.VanReconcileCompleted;
import kr.co.pg.batch.reconcile.van.k.job.parameters.KVanReconcileJobParameters;
import kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper;
import kr.co.pg.batch.reconcile.van.k.step.listener.KVanReconcileAndTransitionStepExecutionListener;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
public class KVanReconcileAndTransitionStepConfig {
    private final String RECONCILE_VAN_INSERT_PAYMENT_MAIN_STEP = KVanReconcileBatchConfig.PREFIX + "InsertPaymentMainStep";
    private final String RECONCILE_VAN_UPDATE_PAYMENT_MAIN_STEP = KVanReconcileBatchConfig.PREFIX + "UpdatePaymentMainStep";
    private final String RECONCILE_VAN_INSERT_PAYMENT_ADDINFO_STEP = KVanReconcileBatchConfig.PREFIX + "InsertPaymentAddinfoStep";
    private final String RECONCILE_VAN_INSERT_PAYMENT_DETAIL_STEP = KVanReconcileBatchConfig.PREFIX + "InsertPaymentDetailStep";
    private final String RECONCILE_VAN_CHECK_CUSTID_STEP = KVanReconcileBatchConfig.PREFIX + "checkCustidStep";
    private final String RECONCILE_VAN_CHECK_ORDERNO_STEP = KVanReconcileBatchConfig.PREFIX + "checkOrderNoStep";
    private final String RECONCILE_VAN_STEP = KVanReconcileBatchConfig.PREFIX + "Step";

    private final String RECONCILE_VAN_GET_QUERY_ID = "kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper.getReconcileVan";
    private final String RECONCILE_VAN_INSERT_QUERY_ID = "kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper.insertReconcileVan";

    private final String PAYMENT_MAIN_GET_QUERY_ID = "kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper.getReconcileVanForPaymentMain";
    private final String PAYMENT_MAIN_INSERT_QUERY_ID = "kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper.insertPaymentMain";

    private final String PAYMENT_ADDINFO_GET_QUERY_ID = "kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper.getReconcileVanForPaymentAddinfo";
    private final String PAYMENT_ADDINFO_INSERT_QUERY_ID = "kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper.insertPaymentAddinfo";

    private final String PAYMENT_DETAIL_GET_QUERY_ID = "kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper.getReconcileVanForPaymentDetail";
    private final String PAYMENT_DETAIL_INSERT_QUERY_ID = "kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper.insertPaymentDetail";

    private final String RECONCILE_VAN_ORDERNO_CHECK_QUERY_ID = "kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper.getReconcileVanForUpdatePaymentMain";
    private final String RECONCILE_VAN_INSERT_UPDATE_QUERY_ID = "kr.co.pg.batch.reconcile.common.mapper.PaymentMainMapper.insertUpdatePaymentMain";
    private final int CHUNK_SIZE = 100;

    private final StepBuilderFactory stepBuilderFactory;
    private final SqlSessionFactory sqlSessionFactory;
    private final KVanReconcileJobParameters kVanReconcileJobParameters;
    private final KVanReconcileAndTransitionStepExecutionListener kVanReconcileAndTransitionStepExecutionListener;
    private final ReconcileVanMapper reconcileVanMapper;
    private final PaymentMainMapper paymentMainMapper;
    private final PaymentDetailRepository paymentDetailRepository;

    private final KReconcileAndTransitionPaymentMainStepExecutionListener kReconcileAndTransitionPaymentMainStepExecutionListener;

    private final CommonStepLoggingListener commonStepLoggingListener;

    private final DateTimeFormatter STANDARD_DATE_FORMATTER = DateFormatUtils.DASH_DATE;
    private final String PAYMENT_ROUTE_VAN = "VAN";

    private final Integer dbConfiguration;

    @Autowired
    public KVanReconcileAndTransitionStepConfig(StepBuilderFactory stepBuilderFactory, SqlSessionFactory sqlSessionFactory
            , KVanReconcileJobParameters kVanReconcileJobParameters
            , KVanReconcileAndTransitionStepExecutionListener kVanReconcileAndTransitionStepExecutionListener
            , ReconcileVanMapper reconcileVanMapper
            , PaymentMainMapper paymentMainMapper, PaymentDetailRepository paymentDetailRepository, KReconcileAndTransitionPaymentMainStepExecutionListener kReconcileAndTransitionPaymentMainStepExecutionListener
            , CommonStepLoggingListener commonStepLoggingListener
            , @Value("${db-configuration}") Integer dbConfiguration
    ) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.sqlSessionFactory = sqlSessionFactory;
        this.kVanReconcileJobParameters = kVanReconcileJobParameters;
        this.paymentMainMapper = paymentMainMapper;
        this.paymentDetailRepository = paymentDetailRepository;
        this.kReconcileAndTransitionPaymentMainStepExecutionListener = kReconcileAndTransitionPaymentMainStepExecutionListener;
        this.reconcileVanMapper = reconcileVanMapper;
        this.kVanReconcileAndTransitionStepExecutionListener = kVanReconcileAndTransitionStepExecutionListener;
        this.commonStepLoggingListener = commonStepLoggingListener;
        this.dbConfiguration = dbConfiguration;
    }

    @Bean
    public Step reconcileVanPaymentMainUpdateStep() {
        return stepBuilderFactory.get(RECONCILE_VAN_UPDATE_PAYMENT_MAIN_STEP)
                .tasklet(reconcileVanCompleteBeforeTasklet())
                .build();
    }

    @Bean
    @JobScope
    public Step reconcileVanPaymentMainForCheckOrderNoStep() {
        return stepBuilderFactory.get(RECONCILE_VAN_CHECK_ORDERNO_STEP)
                .listener(kReconcileAndTransitionPaymentMainStepExecutionListener)
                .listener(commonStepLoggingListener)
                .<PaymentMain, PaymentMain>chunk(CHUNK_SIZE)
                .reader(reconcileVanPaymentMainForCheckOrderNoReader(kVanReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .writer(reconcileVanPaymentMainForCheckOrderNoWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PaymentMain> reconcileVanPaymentMainForCheckOrderNoReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);

        return new MyBatisPagingItemReaderBuilder<PaymentMain>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(RECONCILE_VAN_ORDERNO_CHECK_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private MyBatisBatchItemWriter<PaymentMain> reconcileVanPaymentMainForCheckOrderNoWriter() {
        return new MyBatisBatchItemWriterBuilder<PaymentMain>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(RECONCILE_VAN_INSERT_UPDATE_QUERY_ID)
                .build();
    }

    @Bean
    @JobScope
    public Step reconcileVanPaymentMainStep() {
        return stepBuilderFactory.get(RECONCILE_VAN_INSERT_PAYMENT_MAIN_STEP)
                .listener(commonStepLoggingListener)
                .<PaymentMain, PaymentMainDto>chunk(CHUNK_SIZE)
                .reader(reconcileVanPaymentMainReader(kVanReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .processor(entityToDtoProcessor())
                .writer(reconcileVanPaymentMainWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PaymentMain> reconcileVanPaymentMainReader(String standardDate) {
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

    private MyBatisBatchItemWriter<PaymentMainDto> reconcileVanPaymentMainWriter() {
        return new MyBatisBatchItemWriterBuilder<PaymentMainDto>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(PAYMENT_MAIN_INSERT_QUERY_ID)
                .build();
    }

    @Bean
    @JobScope
    public Step reconcileVanPaymentAddinfoStep() {
        return stepBuilderFactory.get(RECONCILE_VAN_INSERT_PAYMENT_ADDINFO_STEP)
                .listener(commonStepLoggingListener)
                .<PaymentAddinfo, PaymentAddinfo>chunk(CHUNK_SIZE)
                .reader(reconcileVanPaymentAddinfoReader(kVanReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .writer(reconcileVanPaymentAddinfoWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PaymentAddinfo> reconcileVanPaymentAddinfoReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);
        return new MyBatisPagingItemReaderBuilder<PaymentAddinfo>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(PAYMENT_ADDINFO_GET_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private MyBatisBatchItemWriter<PaymentAddinfo> reconcileVanPaymentAddinfoWriter() {
        return new MyBatisBatchItemWriterBuilder<PaymentAddinfo>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(PAYMENT_ADDINFO_INSERT_QUERY_ID)
                .build();
    }

    @Bean
    @JobScope
    public Step reconcileVanPaymentDetailStep() {
        return stepBuilderFactory.get(RECONCILE_VAN_INSERT_PAYMENT_DETAIL_STEP)
                .listener(commonStepLoggingListener)
                .<PaymentDetail, PaymentDetail>chunk(CHUNK_SIZE)
                .reader(reconcileVanPaymentDetailReader(kVanReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .processor(reconcileVanPaymentDetailProcessor())
                .writer(reconcileVanPaymentDetailWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<PaymentDetail> reconcileVanPaymentDetailReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);
        return new MyBatisPagingItemReaderBuilder<PaymentDetail>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(PAYMENT_DETAIL_GET_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private ItemProcessor<PaymentDetail, PaymentDetail> reconcileVanPaymentDetailProcessor() {
        return item -> {
            List<PaymentDetail> resultDetail = paymentDetailRepository.findByTid(item.getTid());
            if (resultDetail.size() > 0) {
                return null;
            } else {
                return item;
            }
        };
    }

    private MyBatisBatchItemWriter<PaymentDetail> reconcileVanPaymentDetailWriter() {
        return new MyBatisBatchItemWriterBuilder<PaymentDetail>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(PAYMENT_DETAIL_INSERT_QUERY_ID)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet reconcileVanCompleteBeforeTasklet() {
        return (contribution, chunkContext) -> {
            String standardDate = kVanReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER);
            try {
                reconcileVanMapper.updatePaymentMainReconcileInsertCompletedForTid(standardDate);
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to update payment main insert completed for tid from reconcile - reason: ")
                        .append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }
            try {
                reconcileVanMapper.updatePaymentMainReconcileInsertCompletedForOrgTid(standardDate);
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to update payment main insert completed for orgtid from reconcile - reason: ")
                        .append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }
            try {
                reconcileVanMapper.updatePaymentMainReconcileInsertCompleted(standardDate);
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
    public Step reconcileVanStep() {
        return stepBuilderFactory.get(RECONCILE_VAN_STEP)
                .listener(kVanReconcileAndTransitionStepExecutionListener)
                .listener(commonStepLoggingListener)
                .<VanReconcileCompleted, VanReconcileCompleted>chunk(CHUNK_SIZE)
                .reader(reconcileVanReader(kVanReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER)))
                .writer(reconcileVanWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private MyBatisPagingItemReader<VanReconcileCompleted> reconcileVanReader(String standardDate) {
        Map<String, Object> param = new HashMap<>();
        param.put("standardDate", standardDate);
        return new MyBatisPagingItemReaderBuilder<VanReconcileCompleted>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId(RECONCILE_VAN_GET_QUERY_ID)
                .parameterValues(param)
                .pageSize(CHUNK_SIZE)
                .build();
    }

    private MyBatisBatchItemWriter<VanReconcileCompleted> reconcileVanWriter() {
        return new MyBatisBatchItemWriterBuilder<VanReconcileCompleted>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId(RECONCILE_VAN_INSERT_QUERY_ID)
                .build();
    }

    @Bean
    @JobScope
    public Step checkCustidVanStep() {
        return stepBuilderFactory.get(RECONCILE_VAN_CHECK_CUSTID_STEP)
                .tasklet(checkCustidVanTasklet())
                .build();
    }

    @Bean
    @StepScope
    public Tasklet checkCustidVanTasklet() {
        return (contribution, chunkContext) -> {
            String standardDate = kVanReconcileJobParameters.getStandardDate().format(STANDARD_DATE_FORMATTER);
            List<VanCheckCustidSet> custidList = reconcileVanMapper.selectVanCustidListForCheck(standardDate);
            for (VanCheckCustidSet custidSet : custidList) {
                if (StringUtils.equals(custidSet.getResult(), "1")) {
                    log.error("Failed to check custid exist from van reconcile - [{}]", custidSet);
                    throw new Exception("NoCustidException - terminalNo: " + custidSet.getTerminalNo());
                } else {
                    log.info("custidMap - [{}]", custidSet);
                }
            }
            return RepeatStatus.FINISHED;
        };
    }
}
