package kr.co.pg.batch.reconcile.common.repository;

import kr.co.pg.batch.reconcile.common.domain.entity.IntegratedIssuerAcquirerCode;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface IntegratedIssuerAcquirerCodeRepository extends JpaRepository<IntegratedIssuerAcquirerCode, Long> {
    List<IntegratedIssuerAcquirerCode> findAllByVendorTypeAndPgTypeAndCodeType(String vendorType, String pgType, int codeType);
}
