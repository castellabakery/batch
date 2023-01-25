package kr.co.pg.batch.reconcile.common.mapper;

import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface PaymentMainMapper {

    int initiateTid(Integer dbConfiguration);
}
