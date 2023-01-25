package kr.co.pg.batch.count.monthly.mapper;

import kr.co.pg.batch.count.monthly.domain.CountMonthlyRequestDto;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface CountMonthlyMapper {
    void callProcedureCountMonthly(CountMonthlyRequestDto param);
}
