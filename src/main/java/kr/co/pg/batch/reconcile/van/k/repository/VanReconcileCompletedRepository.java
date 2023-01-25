package kr.co.pg.batch.reconcile.van.k.repository;

import kr.co.pg.batch.reconcile.van.k.domain.entity.VanReconcileCompleted;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;

@Repository
public interface VanReconcileCompletedRepository extends JpaRepository<VanReconcileCompleted, Long> {


    Page<VanReconcileCompleted> findByPaymentTypeAndVenderTypeAndMigrationStandardDateAndReconcileResult(int paymentType, String venderType, LocalDate standardDate, int reconcileResult, Pageable pageable); // step reader 에서 호출하여 사용

    long deleteByMigrationStandardDate(LocalDate standardDate);

}
