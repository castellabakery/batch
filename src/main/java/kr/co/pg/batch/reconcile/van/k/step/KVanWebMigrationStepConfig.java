package kr.co.pg.batch.reconcile.van.k.step;

import kr.co.pg.batch.reconcile.common.domain.KWebIdSet;
import kr.co.pg.batch.reconcile.common.domain.entity.CardUdrs;
import kr.co.pg.batch.reconcile.common.domain.entity.PaymentMain;
import kr.co.pg.batch.reconcile.common.mapper.CardUdrsMapper;
import kr.co.pg.batch.reconcile.common.repository.PaymentMainRepository;
import kr.co.pg.batch.reconcile.common.service.KWebMigrationService;
import kr.co.pg.batch.reconcile.common.step.listener.CommonStepLoggingListener;
import kr.co.pg.batch.reconcile.pg.k.enums.VenderCodeEnum;
import kr.co.pg.batch.reconcile.van.k.KVanReconcileBatchConfig;
import kr.co.pg.batch.reconcile.van.k.domain.entity.VanReconcileCompleted;
import kr.co.pg.batch.reconcile.van.k.job.parameters.KVanReconcileJobParameters;
import kr.co.pg.batch.reconcile.van.k.repository.VanReconcileCompletedRepository;
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
public class KVanWebMigrationStepConfig {

    private final String STEP_APPROVE_WEB_MIGRATION = KVanReconcileBatchConfig.PREFIX + "ApproveWebMigrationStep";

    private final String STEP_CANCEL_WEB_MIGRATION = KVanReconcileBatchConfig.PREFIX + "CancelWebMigrationStep";

    private final String READER = KVanReconcileBatchConfig.PREFIX + "WebMigrationReader";

    private final StepBuilderFactory stepBuilderFactory;

    private final KVanReconcileJobParameters kVanReconcileJobParameters;

    private final VanReconcileCompletedRepository vanReconcileCompletedRepository;

    private final PaymentMainRepository paymentMainRepository;

    private final VenderCodeEnum VENDOR = VenderCodeEnum.K;

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

    private final String VAN_CREATED_USER_NAME = "AUTO_BATCH_K_VAN";
    private final String SETTLEMENT_UNAFFECTED_WORD = "OFFLINE";

    private final String VAN_PAY_GROUP_CODE = "VAN";

    private final boolean IS_SKIPPED_WHEN_NOT_REGISTERED_SHOP_ID = false;

    @Autowired
    public KVanWebMigrationStepConfig(StepBuilderFactory stepBuilderFactory
            , KVanReconcileJobParameters kVanReconcileJobParameters
            , VanReconcileCompletedRepository vanReconcileCompletedRepository
            , KWebMigrationService kWebMigrationService
            , CommonStepLoggingListener commonStepLoggingListener
            , PaymentMainRepository paymentMainRepository
            , CardUdrsMapper cardUdrsMapper) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.kVanReconcileJobParameters = kVanReconcileJobParameters;
        this.vanReconcileCompletedRepository = vanReconcileCompletedRepository;
        this.kWebMigrationService = kWebMigrationService;
        this.commonStepLoggingListener = commonStepLoggingListener;
        this.paymentMainRepository = paymentMainRepository;
        this.cardUdrsMapper = cardUdrsMapper;
    }

    @Bean(name = STEP_APPROVE_WEB_MIGRATION)
    @JobScope
    public Step migrateApproveWebData() {

        LocalDate standardDate = kVanReconcileJobParameters.getStandardDate();

        return stepBuilderFactory.get(STEP_APPROVE_WEB_MIGRATION)
                .listener(commonStepLoggingListener)
                .<VanReconcileCompleted, CardUdrs>chunk(CHUNK_SIZE)
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

        LocalDate standardDate = kVanReconcileJobParameters.getStandardDate();

        return stepBuilderFactory.get(STEP_CANCEL_WEB_MIGRATION)
                .listener(commonStepLoggingListener)
                .<VanReconcileCompleted, CardUdrs>chunk(CHUNK_SIZE)
                .reader(reconcileCompletedReader(PAYMENT_CANCEL, standardDate))
                .processor(makeCardUdrsProcessor(true))
                .writer(cardUdrsCancelWriter(standardDate))
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private RepositoryItemReader<VanReconcileCompleted> reconcileCompletedReader(int paymentType, LocalDate standardDate) {

        return new RepositoryItemReaderBuilder<VanReconcileCompleted>()
                .name(READER)
                .repository(vanReconcileCompletedRepository)
                .pageSize(CHUNK_SIZE)
                .methodName("findByPaymentTypeAndVenderTypeAndMigrationStandardDateAndReconcileResult")
                .arguments(Arrays.asList(paymentType, VENDOR.name, standardDate, 0))
                .sorts(Collections.singletonMap("seq", Sort.Direction.ASC))
                .build();
    }

    private BigDecimal checkTrAmt(VanReconcileCompleted vanReconcileCompleted) {
        if (Objects.equals(vanReconcileCompleted.getPaymentType(), PAYMENT_APPROVE)) {
            return new BigDecimal(vanReconcileCompleted.getPaymentAmount());
        } else {
            BigDecimal minusDecimal = new BigDecimal(-1);
            BigDecimal amountDecimal = new BigDecimal(vanReconcileCompleted.getPaymentAmount());
            return minusDecimal.multiply(amountDecimal);
        }
    }

    private ItemProcessor<VanReconcileCompleted, CardUdrs> makeCardUdrsProcessor(boolean isCanceled) {
        return item -> {

            // web 등록 시, van의 경우에는 단말기번호를 shop id 로 등록
            KWebIdSet kWebIdSet;
            try {
                kWebIdSet = kWebMigrationService.getWebIds(item.getTerminalId());
            } catch (NotRegisteredShopIdException e) {
                if (IS_SKIPPED_WHEN_NOT_REGISTERED_SHOP_ID) {
                    log.warn("Skipped casue not registered shop - isCanceled: {}, {}", isCanceled, item);
                    return null;
                }
                throw e;
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to get web ids - ")
                        .append(item).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            String issuerCode;
            try {
                issuerCode = kWebMigrationService.getVanOnlineIssuerCode(item.getIssuerCode());
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to get issuer codes - ")
                        .append(item).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            String orgTid = null;
            String purchaseManType = PURCHASE_MAN_TYPE_A;
            String transStatus;
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
                    .paymentType(VENDOR.paymentType)
                    .issueCode(issuerCode)
                    .purchaseCode(kWebIdSet.getOid())
                    .oid(kWebIdSet.getOid())
                    .cardMid(item.getTerminalId())
                    .checkcd(Objects.equals((item.getCardType()), CARD_TYPE_CREDIT) ? "N" : "C")
                    .approvNo(item.getApproveNo())
                    .vanApprOid(SETTLEMENT_UNAFFECTED_WORD)
                    .vanPuchOid(SETTLEMENT_UNAFFECTED_WORD)
                    .vanTid(item.getTerminalId())
                    .payGroup(VAN_PAY_GROUP_CODE)
                    .payMethod(VENDOR.payMethod)
                    .currencyType(VENDOR.currencyType)
                    .quotaMonths(item.getHiremonth())
                    .quotaMonthsType(VENDOR.quotaMonthsType)
                    .cardno(TEncrypt.kEncAesEncrypt(item.getCardNo()))
                    .cardSearch(TEncrypt.kSha512Encrypt(item.getCardNo()))
                    .purchaseType(VENDOR.purchaseType)
                    .opgTid(item.getVenderTxnId())
                    .pgmId(VAN_CREATED_USER_NAME)
                    .crtUser(VAN_CREATED_USER_NAME)
                    .payBrand(DEFAULT_PAY_BRAND)
                    .reserved01(kVanReconcileJobParameters.getStandardDate().toString())
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
                    throw new Exception("Invalid updated result  - expected: 1, actual: " + result + " " + cardUdrs);
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
                        result = cardUdrsMapper.updateCardUdrsTransStatusForCancelToday(cardUdrs.getTid(), VAN_CREATED_USER_NAME, standardDate);
                    } catch (Exception e) {
                        String errorMessage = new StringBuilder("Failed to update card udrs trans status for cancel - ")
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
                        result = cardUdrsMapper.updateCardUdrsPurchasedtForCancelNotToday(cardUdrs.getTid(), VAN_CREATED_USER_NAME, cardUdrs.getPurchasedt());
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
