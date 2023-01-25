package kr.co.pg.batch.reconcile.pg.k.repository;

import kr.co.pg.batch.reconcile.pg.k.domain.entity.KPgReconcile;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Optional;

@Repository
public interface KPgReconcileRepository extends JpaRepository<KPgReconcile, Long> {

    Optional<KPgReconcile> findByShopIdAndPgTradeNoAndApproveNoAndApproveCode(String shopId, String pgTradeNo, String approveNo, String approveCode);

    long deleteByMigrationStandardDate(LocalDate migrationStandardDate);
}
