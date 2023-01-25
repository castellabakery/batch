package kr.co.pg.batch.reconcile.van.k.service;

import kr.co.pg.batch.reconcile.common.mapper.CardUdrsMapper;
import kr.co.pg.batch.reconcile.van.k.mapper.ReconcileVanMapper;
import kr.co.pg.batch.reconcile.van.k.repository.VanReconcileCompletedRepository;
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
public class KVanReconcileDeleteService {

    private final CardUdrsMapper cardUdrsMapper;
    private final ReconcileVanMapper reconcileVanMapper;
    private final VanReconcileCompletedRepository vanReconcileCompletedRepository;

    private final String AUTO_BATCH = "AUTO_BATCH_K_VAN";

    @Autowired
    public KVanReconcileDeleteService(
            CardUdrsMapper cardUdrsMapper
            , ReconcileVanMapper reconcileVanMapper
            , VanReconcileCompletedRepository vanReconcileCompletedRepository) {
        this.cardUdrsMapper = cardUdrsMapper;
        this.reconcileVanMapper = reconcileVanMapper;
        this.vanReconcileCompletedRepository = vanReconcileCompletedRepository;
    }

    @Transactional
    public Map<String, Long> deleteAll(LocalDate standardDate) throws Exception {
        log.info("K Van Reconcile Fail Over Process Started");

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
            result = reconcileVanMapper.deletePaymentDetail(standardDateStr);
        } catch (Exception e) {
            log.error("Failed to delete payment detail data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("paymentDetailDeletedCount", result);
        log.info("Deleted payment detail data - deleted count: {}", result);

        try {
            result = reconcileVanMapper.deletePaymentAddinfo(standardDateStr);
        } catch (Exception e) {
            log.error("Failed to delete payment addinfo data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("paymentAddinfoDeletedCount", result);
        log.info("Deleted payment addinfo data - deleted count: {}", result);

        try {
            result = reconcileVanMapper.deletePaymentMain(standardDateStr);
        } catch (Exception e) {
            log.error("Failed to delete payment main data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("paymentMainDeletedCount", result);
        log.info("Deleted payment main data - deleted count: {}", result);

        try {
            result = vanReconcileCompletedRepository.deleteByMigrationStandardDate(standardDate);
        } catch (Exception e) {
            log.error("Failed to delete van reconcile completed data - standardDate: {}", standardDateStr);
            throw e;
        }
        deletedContent.put("vanReconcileCompletedDeletedCount", result);
        log.info("Deleted van reconcile completed data - deleted count: {}", result);

        log.info("K Van Reconcile Fail Over Process Finished Successfully");
        return deletedContent;
    }
}
