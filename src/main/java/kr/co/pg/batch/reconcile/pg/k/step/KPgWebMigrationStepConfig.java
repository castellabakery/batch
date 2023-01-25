package kr.co.pg.batch.reconcile.pg.k.step;

import kr.co.pg.batch.reconcile.common.domain.KWebIdSet;
import kr.co.pg.batch.reconcile.common.domain.entity.CardUdrs;
import kr.co.pg.batch.reconcile.common.domain.entity.PaymentMain;
import kr.co.pg.batch.reconcile.common.mapper.CardUdrsMapper;
import kr.co.pg.batch.reconcile.common.repository.PaymentMainRepository;
import kr.co.pg.batch.reconcile.common.service.KWebMigrationService;
import kr.co.pg.batch.reconcile.common.step.listener.CommonStepLoggingListener;
import kr.co.pg.batch.reconcile.pg.k.KPgReconcileBatchConfig;
import kr.co.pg.batch.reconcile.pg.k.domain.entity.PgReconcileCompleted;
import kr.co.pg.batch.reconcile.pg.k.enums.VenderCodeEnum;
import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
import kr.co.pg.batch.reconcile.pg.k.repository.PgReconcileCompletedRepository;
import kr.co.pg.exception.reconcile.NotRegisteredShopIdException;
import kr.co.pg.util.StringUtils;
import kr.co.pg.util.TEncrypt;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Configuration
public class KPgWebMigrationStepConfig {

    private final String STEP_APPROVE_WEB_MIGRATION = KPgReconcileBatchConfig.PREFIX + "ApproveWebMigrationStep";

    private final String STEP_CANCEL_WEB_MIGRATION = KPgReconcileBatchConfig.PREFIX + "CancelWebMigrationStep";

    private final String READER = KPgReconcileBatchConfig.PREFIX + "WebMigrationReader";

    private final StepBuilderFactory stepBuilderFactory;

    private final KPgReconcileJobParameters kPgReconcileJobParameters;

    private final PgReconcileCompletedRepository pgReconcileCompletedRepository;

    private final PaymentMainRepository paymentMainRepository;

    private final VenderCodeEnum VENDER = VenderCodeEnum.K;

    private final KWebMigrationService kWebMigrationService;

    private final CardUdrsMapper cardUdrsMapper;

    private final CommonStepLoggingListener commonStepLoggingListener;

    private final int CHUNK_SIZE = 100;

    private final int PAYMENT_APPROVE = 0;
    private final int PAYMENT_CANCEL = 1;

    private final int CARD_TYPE_CREDIT = 0;
    private final String PURCHASE_MAN_TYPE_A = "A";
    private final String PURCHASE_MAN_TYPE_C = "C";

    private final String TRANS_STATUS_CANCEL_TODAY = "40";
    private final String TRANS_STATUS_CANCEL_NOT_TODAY = "53";
    private final String TRANS_STATUS_APPROVE = "20";
    private final String DEFAULT_PAY_BRAND = "OCC";

    private final String PG_CREATED_USER_NAME = "AUTO_BATCH_K_PG";

    private final boolean IS_SKIPPED_WHEN_NOT_REGISTERED_SHOP_ID = false;

    @Autowired
    public KPgWebMigrationStepConfig(StepBuilderFactory stepBuilderFactory
            , KPgReconcileJobParameters kPgReconcileJobParameters
            , PgReconcileCompletedRepository pgReconcileCompletedRepository
            , KWebMigrationService kWebMigrationService
            , CommonStepLoggingListener commonStepLoggingListener
            , CardUdrsMapper cardUdrsMapper
            , PaymentMainRepository paymentMainRepository) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.kPgReconcileJobParameters = kPgReconcileJobParameters;
        this.pgReconcileCompletedRepository = pgReconcileCompletedRepository;
        this.kWebMigrationService = kWebMigrationService;
        this.commonStepLoggingListener = commonStepLoggingListener;
        this.cardUdrsMapper = cardUdrsMapper;
        this.paymentMainRepository = paymentMainRepository;
    }

    @Bean(name = STEP_APPROVE_WEB_MIGRATION)
    @JobScope
    public Step migrateApproveWebData() {

        LocalDate standardDate = kPgReconcileJobParameters.getStandardDate();

        return stepBuilderFactory.get(STEP_APPROVE_WEB_MIGRATION)
                .listener(commonStepLoggingListener)
                .<PgReconcileCompleted, CardUdrs>chunk(CHUNK_SIZE)
                .reader(reconcileCompletedReader(PAYMENT_APPROVE, standardDate))
                .processor(makeCardUdrsProcessor(false))
                .writer(cardUdrsApproveWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    @Bean(name = STEP_CANCEL_WEB_MIGRATION)
    @JobScope
    public Step migrateCancelWebData() {

        LocalDate standardDate = kPgReconcileJobParameters.getStandardDate();

        return stepBuilderFactory.get(STEP_CANCEL_WEB_MIGRATION)
                .listener(commonStepLoggingListener)
                .<PgReconcileCompleted, CardUdrs>chunk(CHUNK_SIZE)
                .reader(reconcileCompletedReader(PAYMENT_CANCEL, standardDate))
                .processor(makeCardUdrsProcessor(true))
                .writer(cardUdrsCancelWriter(standardDate))
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private RepositoryItemReader<PgReconcileCompleted> reconcileCompletedReader(int paymentType, LocalDate standardDate) {

        return new RepositoryItemReaderBuilder<PgReconcileCompleted>()
                .name(READER)
                .repository(pgReconcileCompletedRepository)
                .pageSize(CHUNK_SIZE)
                .methodName("findByPaymentTypeAndVenderTypeAndMigrationStandardDateAndReconcileResult")
                .arguments(Arrays.asList(paymentType, VENDER.name, standardDate, 0))
                .sorts(Collections.singletonMap("seq", Sort.Direction.ASC))
                .build();
    }

    private BigDecimal checkTrAmt(PgReconcileCompleted pgReconcileCompleted) {
        if (Objects.equals(pgReconcileCompleted.getPaymentType(), PAYMENT_APPROVE)) {
            return new BigDecimal(pgReconcileCompleted.getPaymentAmount());
        } else {
            BigDecimal minusDecimal = new BigDecimal(-1);
            BigDecimal amountDecimal = new BigDecimal(pgReconcileCompleted.getPaymentAmount());
            return minusDecimal.multiply(amountDecimal);
        }
    }

    private ItemProcessor<PgReconcileCompleted, CardUdrs> makeCardUdrsProcessor(boolean isCanceled) {
        return item -> {

            KWebIdSet kWebIdSet;
            try {
                kWebIdSet = kWebMigrationService.getWebIds(item.getShopId());
            } catch (NotRegisteredShopIdException e) {
                if (IS_SKIPPED_WHEN_NOT_REGISTERED_SHOP_ID) {
                    log.warn("Skipped casue not registered shop - {}", item);
                    return null;
                }
                throw e;
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to get web ids - ").append(item).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            String issuerCode;
            try {
                issuerCode = kWebMigrationService.getPgOnlineIssuerCode(item.getIssuerCode());
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to get issuer codes - ").append(item).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            String orgTid = null;
            String purchaseManType = PURCHASE_MAN_TYPE_A;
            String transStatus = "";
            LocalDateTime purchaseDt = null;

            if (!isCanceled) {
                transStatus = TRANS_STATUS_APPROVE;
            } else {

                Optional<PaymentMain> paymentMainOptional = paymentMainRepository.findByTid(item.getTxnId());
                if (paymentMainOptional.isEmpty()) {
                    log.error("Nothing matches payment main data - {}", item);
                    throw new Exception("Nothing matches payment main data - " + item);
                }
                PaymentMain paymentMain = paymentMainOptional.get();
                String paymentMainOrgTid = paymentMain.getOrgTid();
                if (StringUtils.isBlank(paymentMainOrgTid)) {
                    log.warn("Not Exist Original Trade History - {}", paymentMain);
                    return null;
                }

                CardUdrs origin;
                try {
                    origin = cardUdrsMapper.selectOriginCardUdrs(paymentMainOrgTid);
                } catch (Exception e) {
                    String errorMessage = new StringBuilder("Failed to select origin card udrs - ")
                            .append(paymentMain).append(", ").append(item)
                            .append(", reason: ").append(e.getMessage()).toString();
                    log.error(errorMessage);
                    throw new Exception(errorMessage);
                }

                if (origin == null) {
                    log.error("Not Exist card udrs Data - {}, {}", paymentMain, item);
                    throw new Exception("Not Exist Card Udrs Data - " + paymentMain + ", " + item);
                }

                if (origin.getTid() == null || origin.getTransdt() == null) {
                    log.error("Invalid origin card udrs data - origin data: {}", origin);
                    throw new Exception("Invalid origin card udrs data - origin data: " + origin);
                }

                orgTid = origin.getTid();
                LocalDate originTradeDate = origin.getTransdt().toLocalDate();
                LocalDate tradeDate = item.getTradeResDatetime().toLocalDate();

                if (originTradeDate.isEqual(tradeDate)) {
                    transStatus = TRANS_STATUS_CANCEL_TODAY;
                } else {
                    purchaseManType = PURCHASE_MAN_TYPE_C;
                    transStatus = TRANS_STATUS_CANCEL_NOT_TODAY;
                    purchaseDt = tradeDate.plusDays(1).atStartOfDay();
                }

            }

            return CardUdrs.ByInsertExceptForTid()
                    .isCancel(isCanceled)
                    .orgTid(orgTid)
                    .purchaseManType(purchaseManType)
                    .eid(kWebIdSet.getEid())
                    .mid(kWebIdSet.getMid())
                    .cid(kWebIdSet.getCid())
                    .textTr(isCanceled ? "99" : "00")
                    .orderno(item.getTxnId())
                    .buyReqamt(new BigDecimal(item.getPaymentAmount()))
                    .trAmt(checkTrAmt(item))
                    .transdt(item.getTradeResDatetime())
                    .transStatus(transStatus)
                    .paymentType(VENDER.paymentType)
                    .issueCode(issuerCode)
                    .purchaseCode(kWebIdSet.getOid())
                    .oid(kWebIdSet.getOid())
                    .cardMid(item.getShopId())
                    .checkcd(Objects.equals((item.getCardType()), CARD_TYPE_CREDIT) ? "N" : "C")
                    .approvNo(item.getApproveNo())
                    .vanApprOid(kWebIdSet.getOid())
                    .vanPuchOid(kWebIdSet.getOid())
                    .vanTid(item.getShopId())
                    .payGroup(VENDER.payGroup)
                    .payMethod(VENDER.payMethod)
                    .currencyType(VENDER.currencyType)
                    .quotaMonths(item.getHiremonth())
                    .quotaMonthsType(VENDER.quotaMonthsType)
                    .cardno(TEncrypt.kEncAesEncrypt(item.getCardNo()))
                    .cardSearch(TEncrypt.kSha512Encrypt(item.getCardNo()))
                    .purchaseType(VENDER.purchaseType)
                    .opgTid(item.getVenderTxnId())
                    .pgmId(VENDER.pgmId)
                    .crtUser(item.getCreatedId())
                    .payBrand(DEFAULT_PAY_BRAND)
                    .reserved01(kPgReconcileJobParameters.getStandardDate().toString())
                    .purchasedt(purchaseDt)
                    .build();
        };
    }

    private ItemWriter<CardUdrs> cardUdrsApproveWriter() {
        return items -> {
            for (CardUdrs item : items) {

                String tid;
                try {
                    tid = cardUdrsMapper.selectTid();
                } catch (Exception e) {
                    String errorMessage = new StringBuilder("Failed to select new tid from db function - reason: ")
                            .append(e.getMessage()).toString();
                    log.error(errorMessage);
                    throw new Exception(errorMessage);
                }

                String orgTid = tid;
                CardUdrs cardUdrs = CardUdrs.ByInsertWithCardUdrs()
                        .isCancel(false)
                        .cardUdrs(item)
                        .tid(tid)
                        .orgTid(orgTid)
                        .build();

                int result;
                try {
                    result = cardUdrsMapper.insertCardUdrs(cardUdrs);
                } catch (Exception e) {
                    String errorMessage = new StringBuilder("Failed to insert card udrs for approve - ")
                            .append(cardUdrs).append(", reason: ").append(e.getMessage()).toString();
                    log.error(errorMessage);
                    throw new Exception(errorMessage);
                }

                if (result != 1) {
                    log.error("Invalid inserted result - expected: 1, actual: {}, {}", result, cardUdrs);
                    throw new Exception("Invalid updated result  - expected: 1, actual: " + result + ", " + cardUdrs);
                }

            }
        };
    }

    private ItemWriter<CardUdrs> cardUdrsCancelWriter(LocalDate standardDate) {
        return items -> {

            for (CardUdrs item : items) {

                String tid;
                try {
                    tid = cardUdrsMapper.selectTid();
                } catch (Exception e) {
                    String errorMessage = new StringBuilder("Failed to select new tid from db function - reason: ")
                            .append(e.getMessage()).toString();
                    log.error(errorMessage);
                    throw new Exception(errorMessage);
                }

                CardUdrs cardUdrs = CardUdrs.ByInsertWithCardUdrs()
                        .isCancel(true)
                        .cardUdrs(item)
                        .tid(tid)
                        .orgTid(item.getOrgTid())
                        .build();

                int result;
                try {
                    result = cardUdrsMapper.insertCardUdrs(cardUdrs);
                } catch (Exception e) {
                    String errorMessage = new StringBuilder("Failed to insert card udrs for cancel - ")
                            .append(cardUdrs).append(", reason: ").append(e.getMessage()).toString();
                    log.error(errorMessage);
                    throw new Exception(errorMessage);
                }

                if (result != 1) {
                    log.error("Invalid inserted result - expected: 1, actual: {}, {}", result, cardUdrs);
                    throw new Exception("Invalid updated result  - expected: 1, actual: " + result + ", " + cardUdrs);
                }

                // 당일 취소건에 대해서만 원거래 update
                if (TRANS_STATUS_CANCEL_TODAY.equals(cardUdrs.getTransStatus())) {

                    try {
                        result = cardUdrsMapper.updateCardUdrsTransStatusForCancelToday(cardUdrs.getTid(), PG_CREATED_USER_NAME, standardDate);
                    } catch (Exception e) {
                        String errorMessage = new StringBuilder("Failed to update card udrs trans status for cancel today- ")
                                .append(cardUdrs).append(", reason: ").append(e.getMessage()).toString();
                        log.error(errorMessage);
                        throw new Exception(errorMessage);
                    }

                    if (result != 1) {
                        log.error("Invalid inserted result - expected: 1, actual: {}, {}", result, cardUdrs);
                        throw new Exception("Invalid updated result  - expected: 1, actual: " + result + ", " + cardUdrs);
                    }

                } else if (TRANS_STATUS_CANCEL_NOT_TODAY.equals(cardUdrs.getTransStatus())) {
                    try {
                        result = cardUdrsMapper.updateCardUdrsPurchasedtForCancelNotToday(cardUdrs.getTid(), PG_CREATED_USER_NAME, cardUdrs.getPurchasedt());
                    } catch (Exception e) {
                        String errorMessage = new StringBuilder("Failed to update card udrs trans status for cancel not today - ")
                                .append(cardUdrs).append(", reason: ").append(e.getMessage()).toString();
                        log.error(errorMessage);
                        throw new Exception(errorMessage);
                    }

                    if (result != 1) {
                        log.error("Invalid inserted result - expected: 1, actual: {}, {}", result, cardUdrs);
                        throw new Exception("Invalid updated result  - expected: 1, actual: " + result + ", " + cardUdrs);
                    }

                }

            }

        };
    }

}
