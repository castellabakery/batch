package kr.co.pg.batch.reconcile.pg.k.step;

import kr.co.pg.batch.reconcile.common.step.listener.CommonStepLoggingListener;
import kr.co.pg.batch.reconcile.pg.k.KPgReconcileBatchConfig;
import kr.co.pg.batch.reconcile.pg.k.domain.entity.KPgReconcile;
import kr.co.pg.batch.reconcile.pg.k.domain.entity.KPgReconcileOrigin;
import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
import kr.co.pg.batch.reconcile.pg.k.repository.KPgReconcileOriginRepository;
import kr.co.pg.batch.reconcile.pg.k.repository.KPgReconcileRepository;
import kr.co.pg.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.util.*;

@Slf4j
@Configuration
public class KPgReconcileOriginParsingStepConfig {

    private final String STEP_ORIGIN_LST_PARSING = KPgReconcileBatchConfig.PREFIX + "OriginLstParsingStep";

    private final String STEP_ORIGIN_EDI_PARSING = KPgReconcileBatchConfig.PREFIX + "OriginEdiParsingStep";

    private final String READER = KPgReconcileBatchConfig.PREFIX + "OriginParsingReader";

    private final StepBuilderFactory stepBuilderFactory;

    private final KPgReconcileOriginRepository kPgReconcileOriginRepository;

    private final KPgReconcileRepository kPgReconcileRepository;

    private final KPgReconcileJobParameters kPgReconcileJobParameters;

    private final CommonStepLoggingListener commonStepLoggingListener;

    private final String PURCHASE_NOT_COMPLETED = "N";
    private final String PURCHASE_COMPLETED = "Y";

    private final String PG_RECONCILE_TYPE_LST = "0";
    private final String PG_RECONCILE_TYPE_EDI = "1";

    private final int CHUNK_SIZE = 100;


    @Autowired
    public KPgReconcileOriginParsingStepConfig(StepBuilderFactory stepBuilderFactory
            , KPgReconcileOriginRepository kPgReconcileOriginRepository
            , KPgReconcileRepository kPgReconcileRepository
            , KPgReconcileJobParameters kPgReconcileJobParameters
            , CommonStepLoggingListener commonStepLoggingListener) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.kPgReconcileOriginRepository = kPgReconcileOriginRepository;
        this.kPgReconcileRepository = kPgReconcileRepository;
        this.kPgReconcileJobParameters = kPgReconcileJobParameters;
        this.commonStepLoggingListener = commonStepLoggingListener;
    }

    @Bean(name = STEP_ORIGIN_LST_PARSING)
    @JobScope
    public Step parseOriginLst() {

        LocalDate standardDate = kPgReconcileJobParameters.getStandardDate();
        String adminId = kPgReconcileJobParameters.getAdminId();

        return stepBuilderFactory.get(STEP_ORIGIN_LST_PARSING)
                .listener(commonStepLoggingListener)
                .<KPgReconcileOrigin, KPgReconcile>chunk(CHUNK_SIZE)
                .reader(originParsingReader(PG_RECONCILE_TYPE_LST, standardDate))
                .processor(originParsingProcessor(standardDate, adminId, PURCHASE_NOT_COMPLETED))
                .writer(originToLstParsingWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    @Bean(name = STEP_ORIGIN_EDI_PARSING)
    @JobScope
    public Step parseOriginEdi() {

        LocalDate standardDate = kPgReconcileJobParameters.getStandardDate();
        String adminId = kPgReconcileJobParameters.getAdminId();

        return stepBuilderFactory.get(STEP_ORIGIN_EDI_PARSING)
                .listener(commonStepLoggingListener)
                .<KPgReconcileOrigin, KPgReconcile>chunk(CHUNK_SIZE)
                .reader(originParsingReader(PG_RECONCILE_TYPE_EDI, standardDate))
                .processor(originParsingProcessor(standardDate, adminId, PURCHASE_COMPLETED))
                .writer(originToEdiParsingWriter(adminId))
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private RepositoryItemReader<KPgReconcileOrigin> originParsingReader(String reconcileType, LocalDate standardDate) {
        return new RepositoryItemReaderBuilder<KPgReconcileOrigin>()
                .name(READER)
                .repository(kPgReconcileOriginRepository)
                .pageSize(CHUNK_SIZE)
                .methodName("findByReconcileTypeAndFileDate")
                .arguments(Arrays.asList(reconcileType, standardDate))
                .sorts(Collections.singletonMap("seq", Sort.Direction.ASC))
                .build();
    }

    private ItemProcessor<KPgReconcileOrigin, KPgReconcile> originParsingProcessor(LocalDate standardDate, String adminId, String purchaseYn) {
        return item -> {
            KPgReconcile reconcile;
            try {
                reconcile = KPgReconcile.ByInsertForKPgReconcileOrigin()
                        .kPgReconcileOrigin(item)
                        .purchaseYn(purchaseYn)
                        .createdId(adminId)
                        .migrationStandardDate(standardDate)
                        .build();
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to convert KPgReconcileOrigin to KPgReconcile - ")
                        .append(item).append(", reason: ").append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            if (StringUtils.isBlank(reconcile.getApproveNo())) {
                log.info("Skip Reconcile Original Parsing Data There is no approve no- {}", reconcile);
                return null;
            }

            return reconcile;
        };
    }

    private ItemWriter<KPgReconcile> originToLstParsingWriter() {
        return new RepositoryItemWriterBuilder<KPgReconcile>()
                .repository(kPgReconcileRepository)
                .build();
    }

    private ItemWriter<KPgReconcile> originToEdiParsingWriter(String adminId) {
        return items -> {

            final String SUCCESS_PURCHASE_REJECT_CODE = "00";
            final String SUCCESS_PURCHASE_YN = "Y";
            final String REJECT_PURCHASE_YN = "R";

            if (CollectionUtils.isEmpty(items)) {
                log.warn("Nothing to save KPgReconcile");
                return;
            }

            List<KPgReconcile> savedList = new ArrayList<>();

            for (KPgReconcile item : items) {

                Optional<KPgReconcile> reconcileOptional;
                try {
                    reconcileOptional = kPgReconcileRepository.findByShopIdAndPgTradeNoAndApproveNoAndApproveCode(item.getShopId(), item.getPgTradeNo(), item.getApproveNo(), item.getApproveCode());
                } catch (Exception e) {
                    String errorMessage = new StringBuilder("Failed to select origin lst reconcile - shopId: ").append(item.getShopId())
                            .append(", pgTradeNo: ").append(item.getPgTradeNo()).append(", approveNo: ").append(item.getApproveNo())
                            .append(", amount: ").append(item.getAmount()).append(", reason: ").append(e.getMessage()).toString();
                    log.error(errorMessage);
                    throw new Exception(errorMessage);
                }

                reconcileOptional.ifPresentOrElse(
                        reconcile -> {
                            boolean isInvalid = false;
                            KPgReconcile modified = null;
                            try {
                                modified = KPgReconcile.ByUpdate()
                                        .seq(reconcile.getSeq())
                                        .dataType(item.getDataType())
                                        .shopId(item.getShopId())
                                        .pgTradeNo(item.getPgTradeNo())
                                        .approveCode(item.getApproveCode())
                                        .interestType(item.getInterestType())
                                        .tradeDatetime(item.getTradeDatetime())
                                        .cardNo(item.getCardNo())
                                        .issuerCode(item.getIssuerCode())
                                        .acquirerCode(item.getAcquirerCode())
                                        .expirationPeriod(item.getExpirationPeriod())
                                        .hiremonth(item.getHiremonth())
                                        .amount(item.getAmount())
                                        .approveNo(item.getApproveNo())
                                        .storeNo(item.getStoreNo())
                                        .tradeResCode(item.getTradeResCode())
                                        .orderNo(reconcile.getOrderNo())
                                        .ordererName(item.getOrdererName())
                                        .prodName(item.getProdName())
                                        .purchaseRejectCode(item.getPurchaseRejectCode())
                                        .purchaseReqDate(item.getPurchaseReqDate())
                                        .purchaseFinishDate(item.getPurchaseFinishDate())
                                        .paymentDueDate(item.getPaymentDueDate())
                                        .fee(item.getFee())
                                        .feeVat(item.getFeeVat())
                                        .cardType(item.getCardType())
                                        .moneyCode(item.getMoneyCode())
                                        .exchangeRate(item.getExchangeRate())
                                        .exchangeAmount(item.getExchangeAmount())
                                        .partialCancelSeq(item.getPartialCancelSeq())
                                        .allianceCode(item.getAllianceCode())
                                        .purchaseYn(SUCCESS_PURCHASE_REJECT_CODE.equals(item.getPurchaseRejectCode()) ? SUCCESS_PURCHASE_YN : REJECT_PURCHASE_YN)
                                        .createdId(reconcile.getCreatedId())
                                        .createdDatetime(reconcile.getCreatedDatetime())
                                        .modifiedId(adminId)
                                        .migrationStandardDate(item.getMigrationStandardDate())
                                        .build();
                            } catch (Exception e) {
                                log.error("Failed to build entity KPgReconcile for update - item: {}, reason: {}", item, e.getMessage());
                                isInvalid = true;
                            }

                            if (!isInvalid) {
                                savedList.add(modified);
                            }

                        },
                        () -> {
                            savedList.add(item);
                        }
                );

            }

            if (savedList.size() != items.size()) {
                log.error("Not same list size (Maybe failed to build entity) - expected: {}, actual: {}", items.size(), savedList.size());
                throw new Exception("Failed to build entity for update");
            }

            try {
                kPgReconcileRepository.saveAll(savedList);
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to save edi parsed reconcile list - size: ").append(savedList.size())
                        .append(", reason: ").append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

        };
    }

}
