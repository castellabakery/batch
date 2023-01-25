package kr.co.pg.batch.count.monthly.job.parameters;

import kr.co.pg.api.JobParametersDetail;
import kr.co.pg.api.domain.dto.JobParametersDetailDto;
import kr.co.pg.batch.DefaultJobParameters;
import kr.co.pg.util.DateFormatUtils;
import kr.co.pg.util.StringUtils;
import lombok.Getter;
import lombok.ToString;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
public class CountMonthlyJobParameters implements DefaultJobParameters, JobParametersDetail {

    public static final DateTimeFormatter FORMATTER_YEAR_MONTH = DateFormatUtils.DASH_YEAR_MONTH;

    private String biDate;
    private String regId;

    private final String biDateKey = "biDate";
    private final String regIdKey = "regId";

    public CountMonthlyJobParameters() {}

    public CountMonthlyJobParameters(String biDate, String regId) {
        setBiDate(biDate);
        setRegId(regId);
    }
    public CountMonthlyJobParameters(String regId) {
        this(LocalDate.now().minusDays(1).format(FORMATTER_YEAR_MONTH), regId);
    }

    @Override
    public JobParameters toJobParameter() {
        return new JobParametersBuilder()
                .addString(biDateKey, biDate)
                .addString(regIdKey, regId)
                .toJobParameters();
    }

    @Override
    public void validate() throws IllegalArgumentException {

        if (StringUtils.isBlank(biDate)) {
            throw new IllegalArgumentException("biDate is blank");
        }

        validateBiDate(biDate);

        if (StringUtils.isBlank(regId)) {
            throw new IllegalArgumentException("regId is blank");
        }

    }

    public void setBiDate(String biDate) {
        this.biDate = validateBiDate(biDate);
    }

    public void setRegId(String regId) {
        this.regId = regId;
    }

    private String validateBiDate(String biDate) {
        try {
            FORMATTER_YEAR_MONTH.parse(biDate);
        } catch (Exception e) {
            throw new IllegalArgumentException("biDate is invalid format");
        }
        return biDate;
    }

    @Override
    public List<JobParametersDetailDto> getJobParametersDetailList() {
        List<JobParametersDetailDto> list = new ArrayList<>();

        JobParametersDetailDto biDate = new JobParametersDetailDto(biDateKey, "기준년월", "YYYY-MM 형식");
        JobParametersDetailDto regId = new JobParametersDetailDto(regIdKey, "배치실행 관리자ID", "");

        list.add(biDate);
        list.add(regId);

        return list;
    }
}
