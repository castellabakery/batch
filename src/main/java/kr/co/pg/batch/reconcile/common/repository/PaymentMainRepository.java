package kr.co.pg.batch.reconcile.common.repository;

import kr.co.pg.batch.reconcile.common.domain.entity.PaymentMain;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PaymentMainRepository extends JpaRepository<PaymentMain, Long> {
    Optional<PaymentMain> findByTid(String tid);
}
