package kr.co.pg.batch.reconcile.van.k.mapper;

import kr.co.pg.batch.reconcile.van.k.domain.VanCheckCustidSet;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface ReconcileVanMapper {

    int insertVanReconcileHistoryData(String standardDate);

    int updatePaymentMainReconcileInsertCompleted(String standardDate);

    int updatePaymentMainReconcileInsertCompletedForTid(String standardDate);

    int updatePaymentMainReconcileInsertCompletedForOrgTid(String standardDate);


    int deletePaymentMain(String standardDate);

    int deletePaymentDetail(String standardDate);

    int deletePaymentAddinfo(String standardDate);

    List<VanCheckCustidSet> selectVanCustidListForCheck(String standardDate);

}
