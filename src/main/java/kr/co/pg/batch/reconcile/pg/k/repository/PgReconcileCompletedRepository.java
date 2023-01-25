package kr.co.pg.batch.reconcile.pg.k.repository;

import kr.co.pg.batch.reconcile.pg.k.domain.entity.PgReconcileCompleted;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;

@Repository
public interface PgReconcileCompletedRepository extends JpaRepository<PgReconcileCompleted, Long> {


	Page<PgReconcileCompleted> findByPaymentTypeAndVenderTypeAndMigrationStandardDateAndReconcileResult(int paymentType, String venderType, LocalDate standardDate, int reconcileResult, Pageable pageable); // step reader 에서 호출하여 사용

	long deleteByMigrationStandardDate(LocalDate standardDate);

}
