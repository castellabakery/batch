package kr.co.pg.batch.count.daily.mapper;

import kr.co.pg.batch.count.daily.domain.CountDailyRequestDto;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface CountDailyMapper {

    void callProcedureCountDaily(CountDailyRequestDto param);
}
