package kr.co.pg.batch.reconcile.common.repository;

import kr.co.pg.batch.reconcile.common.domain.entity.PaymentDetail;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PaymentDetailRepository extends JpaRepository<PaymentDetail, Long> {
    List<PaymentDetail> findByTid(String tid);
}
