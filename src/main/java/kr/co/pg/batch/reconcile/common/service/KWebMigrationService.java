package kr.co.pg.batch.reconcile.common.service;

import kr.co.pg.batch.reconcile.common.domain.KIssuerCodeSet;
import kr.co.pg.batch.reconcile.common.domain.KWebIdSet;
import kr.co.pg.batch.reconcile.common.domain.entity.IntegratedIssuerAcquirerCode;
import kr.co.pg.batch.reconcile.common.mapper.CardUdrsMapper;
import kr.co.pg.batch.reconcile.common.repository.IntegratedIssuerAcquirerCodeRepository;
import kr.co.pg.batch.reconcile.pg.k.enums.VenderCodeEnum;
import kr.co.pg.batch.reconcile.pg.k.mapper.ReconcilePgMapper;
import kr.co.pg.exception.reconcile.NotRegisteredShopIdException;
import kr.co.pg.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Slf4j
@Component
public class KWebMigrationService {

    private List<KIssuerCodeSet> kIssuerCodeSetList;

    private List<KWebIdSet> kWebIdSetList;

    private final ReconcilePgMapper reconcilePgMapper;
    private final CardUdrsMapper cardUdrsMapper;

    private final IntegratedIssuerAcquirerCodeRepository integratedIssuerAcquirerCodeRepository;

    @Autowired
    public KWebMigrationService(CardUdrsMapper cardUdrsMapper, ReconcilePgMapper reconcilePgMapper
            , IntegratedIssuerAcquirerCodeRepository integratedIssuerAcquirerCodeRepository) {
        this.reconcilePgMapper = reconcilePgMapper;
        this.cardUdrsMapper = cardUdrsMapper;
        this.integratedIssuerAcquirerCodeRepository = integratedIssuerAcquirerCodeRepository;
    }

    public KWebIdSet getWebIds(String shopId) throws Exception {

        try {
            kWebIdSetList = cardUdrsMapper.selectIds(""); //원하는 날짜 or "" : 전체 리스트
        } catch (Exception e) {
            log.error("Failed to get web ids - reason: {}", e.getMessage());
            throw e;
        }

        for (KWebIdSet kWebIdSet : kWebIdSetList) {
            if (Objects.equals(kWebIdSet.getMerchantNo(), shopId)) {
                return kWebIdSet;
            }
        }

        log.error("No matching id - shopId: {}", shopId);
        throw new NotRegisteredShopIdException("No matching id - shopId: " + shopId);
    }

    public String getPgOnlineIssuerCode(String vendorIssuerCode) throws Exception {

        try {
            kIssuerCodeSetList = reconcilePgMapper.selectMappingIssuerCode(VenderCodeEnum.K.name);
        } catch (Exception e) {
            log.error("Failed to get issuer code - reason: {}", e.getMessage());
            throw e;
        }

        for (KIssuerCodeSet kIssuerCodeSet : kIssuerCodeSetList) {
            if (Objects.equals(kIssuerCodeSet.getOffIssuerCode(), vendorIssuerCode)) {
                return kIssuerCodeSet.getOnIssuerCode();
            }
        }

        log.error("No matching code - vendorIssuerCode: {}", vendorIssuerCode);
        throw new Exception("No matching code - vendorIssuerCode: " + vendorIssuerCode);
    }

    public String getVanOnlineIssuerCode(String vendorIssuerCode) throws Exception {

        if (StringUtils.isBlank(vendorIssuerCode)) {
            log.error("vendorIssuerCode is blank");
            throw new Exception("vendorIssuerCode is blank");
        }

        List<IntegratedIssuerAcquirerCode> integratedIssuerAcquirerCodes;
        try {
            integratedIssuerAcquirerCodes = integratedIssuerAcquirerCodeRepository.findAllByVendorTypeAndPgTypeAndCodeType(VenderCodeEnum.K.name, "VAN", 1);
        } catch (Exception e) {
            log.error("Failed to get integrated issuer code - reason: {}", e.getMessage());
            throw e;
        }

        for (IntegratedIssuerAcquirerCode code : integratedIssuerAcquirerCodes) {
            if (vendorIssuerCode.equals(code.getOfflineCode())) {
                return code.getOnlineCode();
            }
        }

        log.error("No matching code - vendorIssuerCode: {}", vendorIssuerCode);
        throw new Exception("No matching code - vendorIssuerCode: " + vendorIssuerCode);
    }

}
