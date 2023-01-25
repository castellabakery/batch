package kr.co.pg.batch.reconcile.pg.k.repository;

import kr.co.pg.batch.reconcile.pg.k.domain.entity.KPgReconcileFile;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;

@Repository
public interface KPgReconcileFileRepository extends JpaRepository<KPgReconcileFile, Long> {

    KPgReconcileFile findFirstByFileTypeAndFileDateOrderBySeqDesc(String fileType, LocalDate fileDate);
}
