package kr.co.pg.batch.reconcile.common.mapper;

import kr.co.pg.batch.reconcile.common.domain.entity.CardUdrs;
import kr.co.pg.batch.reconcile.common.domain.KWebIdSet;
import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Repository("kr.co.pg.batch.reconcile.common.mapper.CardUdrsMapper")
public class CardUdrsMapper {

    private final SqlSession sqlSession;
    private final String namespace = "kr.co.pg.batch.reconcile.common.mapper.CardUdrsMapper.";

    public CardUdrsMapper(@Qualifier("secondarySqlSessionTemplate") SqlSession sqlSession) {
        this.sqlSession = sqlSession;
    }

    public String selectTid() {
        return this.sqlSession.selectOne(namespace + "selectTid");
    }

    public CardUdrs selectOriginCardUdrs(String orderNo) {
        return this.sqlSession.selectOne(namespace + "selectOriginCardUdrs", orderNo);
    }

    public int insertCardUdrs(CardUdrs cardUdrs) {
        return this.sqlSession.insert(namespace + "insertCardUdrs", cardUdrs);
    }

    public int updateCardUdrsTransStatusForCancelToday(String tid, String updtUser, LocalDate standardDate) {
        CardUdrs cardUdrs = CardUdrs.ByUpdate().updtUser(updtUser).tid(tid).standardDate(standardDate).build();
        return this.sqlSession.update(namespace + "updateCardUdrsTransStatusForCancelToday", cardUdrs);
    }

    public int updateCardUdrsPurchasedtForCancelNotToday(String tid, String updtUser, LocalDateTime purchasedt) {
        CardUdrs cardUdrs = CardUdrs.ByUpdateWithPurchasedt().updtUser(updtUser).tid(tid).purchasedt(purchasedt).build();
        return this.sqlSession.update(namespace + "updateCardUdrsPurchasedtForCancelNotToday", cardUdrs);
    }

    public List<KWebIdSet> selectIds(String shopId) {
        return this.sqlSession.selectList(namespace + "selectIds", shopId);
    }

    public int countByReserved01AndTransStatusAndCrtUser(String reserved01, String transStatus, String crtUser) {
        CardUdrs cardUdrs = CardUdrs.BySelect().reserved01(reserved01).transStatus(transStatus).crtUser(crtUser).build();
        return this.sqlSession.selectOne(namespace + "countByReserved01AndTransStatusAndCrtUser", cardUdrs);
    }

    public long deleteByReserved01AndCrtUser(String reserved01, String crtUser) {
        CardUdrs cardUdrs = CardUdrs.BySelect().reserved01(reserved01).crtUser(crtUser).build();
        return this.sqlSession.delete(namespace + "deleteByReserved01AndCrtUser", cardUdrs);
    }

    public int countCurrentStartingBatch() {
        return this.sqlSession.selectOne(namespace + "countCurrentStartingBatch");
    }
}
