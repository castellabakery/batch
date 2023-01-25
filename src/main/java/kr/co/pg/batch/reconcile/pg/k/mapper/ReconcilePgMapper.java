package kr.co.pg.batch.reconcile.pg.k.mapper;

import kr.co.pg.batch.reconcile.common.domain.KIssuerCodeSet;
import kr.co.pg.batch.reconcile.pg.k.domain.PgCheckCustidSet;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface ReconcilePgMapper {
    int insertPgReconcileHistoryData(String standardDate);

    int updatePaymentMainReconcileInsertCompleted(String standardDate);

    int updatePaymentMainReconcileInsertCompletedForTid(String standardDate);

    int updatePaymentMainReconcileInsertCompletedForOrgTid(String standardDate);

    int deletePaymentMain(String standardDate);

    int deletePaymentDetail(String standardDate);

    int deletePaymentAddinfo(String standardDate);

    List<KIssuerCodeSet> selectMappingIssuerCode(String vendorType);

    List<PgCheckCustidSet> selectPgCustidListForCheck(String standardDate);
}
