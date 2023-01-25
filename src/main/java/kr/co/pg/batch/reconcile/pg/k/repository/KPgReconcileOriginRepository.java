package kr.co.pg.batch.reconcile.pg.k.repository;

import kr.co.pg.batch.reconcile.pg.k.domain.entity.KPgReconcileOrigin;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;

@Repository
public interface KPgReconcileOriginRepository extends JpaRepository<KPgReconcileOrigin, Long> {

    Page<KPgReconcileOrigin> findByReconcileTypeAndFileDate(String reconcileType, LocalDate fileDate, Pageable pageable); // step reader 에서 호출하여 사용

	long deleteByFileDate(LocalDate fileDate);
}
