package kr.co.pg.batch.reconcile.pg.k.service;

import kr.co.pg.batch.reconcile.common.mapper.CardUdrsMapper;
import kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper;
import kr.co.pg.batch.reconcile.pg.k.repository.KPgReconcileOriginRepository;
import kr.co.pg.batch.reconcile.pg.k.repository.KPgReconcileRepository;
import kr.co.pg.batch.reconcile.pg.k.repository.PgReconcileCompletedRepository;
import kr.co.pg.util.DateFormatUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.transaction.Transactional;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class KPgReconcileDeleteService {

    private final CardUdrsMapper cardUdrsMapper;
    private final ReconcilePgMapper reconcilePgMapper;
    private final KPgReconcileRepository kPgReconcileRepository;
    private final PgReconcileCompletedRepository pgReconcileCompletedRepository;
    private final KPgReconcileOriginRepository kPgReconcileOriginRepository;

    private final String NOT_DELETE_TRANS_STATUS = "30";
    private final String AUTO_BATCH = "AUTO_BATCH_K_PG";

    @Autowired
    public KPgReconcileDeleteService(
            CardUdrsMapper cardUdrsMapper
            , PgReconcileCompletedRepository pgReconcileCompletedRepository
            , ReconcilePgMapper reconcilePgMapper
            , KPgReconcileRepository kPgReconcileRepository
            , KPgReconcileOriginRepository kPgReconcileOriginRepository) {
        this.cardUdrsMapper = cardUdrsMapper;
        this.pgReconcileCompletedRepository = pgReconcileCompletedRepository;
        this.reconcilePgMapper = reconcilePgMapper;
        this.kPgReconcileRepository = kPgReconcileRepository;
        this.kPgReconcileOriginRepository = kPgReconcileOriginRepository;
    }


    @Transactional
    public Map<String, Long> deleteAll(LocalDate standardDate) throws Exception {
        log.info("K Pg Reconcile Fail Over Process Started");

        if (standardDate == null) {
            log.error("standardDate must be not null");
            throw new Exception("standardDate must be not null");
        }

        String standardDateStr = standardDate.format(DateFormatUtils.DASH_DATE);

        int staringBatchCount = cardUdrsMapper.countCurrentStartingBatch();
        if (staringBatchCount > 0) {
            log.warn("Now settlement batch is running for ");
            throw new Exception("Now settlement batch is running for ");
        }

        Map<String, Long> deletedContent = new HashMap<>();

        int reconciledCount;
        try {
            reconciledCount = cardUdrsMapper.countByReserved01AndTransStatusAndCrtUser(standardDateStr, NOT_DELETE_TRANS_STATUS, AUTO_BATCH);
        } catch (Exception e) {
            log.error("Failed to get count reconciled case - standardDate: {}", standardDateStr);
            throw e;
        }

        if (reconciledCount > 0) {
            log.warn("reconcile process is done for  - reconciled count: {}", reconciledCount);
            throw new Exception("Reconcile process is done for ");
        }

        long result;
        try {
            result = cardUdrsMapper.deleteByReserved01AndCrtUser(standardDateStr, AUTO_BATCH);
        } catch (Exception e) {
            log.error("Failed to delete card udrs data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("cardUdrsDeletedCount", result);
        log.info("Deleted card udrs data - deleted count: {}", result);

        try {
            result = reconcilePgMapper.deletePaymentDetail(standardDateStr);
        } catch (Exception e) {
            log.error("Failed to delete payment detail data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("paymentDetailDeletedCount", result);
        log.info("Deleted payment detail data - deleted count: {}", result);

        try {
            result = reconcilePgMapper.deletePaymentAddinfo(standardDateStr);
        } catch (Exception e) {
            log.error("Failed to delete payment addinfo data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("paymentAddinfoDeletedCount", result);
        log.info("Deleted payment addinfo data - deleted count: {}", result);

        try {
            result = reconcilePgMapper.deletePaymentMain(standardDateStr);
        } catch (Exception e) {
            log.error("Failed to delete payment main data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("paymentMainDeletedCount", result);
        log.info("Deleted payment main data - deleted count: {}", result);

        try {
            result = pgReconcileCompletedRepository.deleteByMigrationStandardDate(standardDate);
        } catch (Exception e) {
            log.error("Failed to delete pg reconcile completed data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("pgReconcileCompletedDeletedCount", result);
        log.info("Deleted pg reconcile completed data - deleted count: {}", result);

        try {
            result = kPgReconcileRepository.deleteByMigrationStandardDate(standardDate);
        } catch (Exception e) {
            log.error("Failed to delete k reconcile data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("kPgReconcileDeletedCount", result);
        log.info("Deleted k pg reconcile data - deleted count: {}", result);

        try {
            result = kPgReconcileOriginRepository.deleteByFileDate(standardDate);
        } catch (Exception e) {
            log.error("Failed to delete k reconcile origin data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("kPgReconcileOriginDeletedCount", result);
        log.info("Deleted k pg reconcile origin data - deleted count: {}", result);

        log.info("K Pg Reconcile Fail Over Process Finished Successfully");
        return deletedContent;
    }
}
