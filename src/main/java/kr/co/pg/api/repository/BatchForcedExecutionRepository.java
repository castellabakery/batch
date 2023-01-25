package kr.co.pg.api.repository;

import kr.co.pg.api.domain.entity.BatchForcedExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BatchForcedExecutionRepository extends JpaRepository<BatchForcedExecution, Long> {
}
