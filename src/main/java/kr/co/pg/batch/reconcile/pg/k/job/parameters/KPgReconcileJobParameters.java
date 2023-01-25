package kr.co.pg.batch.reconcile.pg.k.job.parameters;

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
public class KPgReconcileJobParameters implements DefaultJobParameters, JobParametersDetail {

    public static final DateTimeFormatter DATE_FORMATTER = DateFormatUtils.DASH_DATE;

    private LocalDate standardDate;

    private String adminId;

    private final String standardDateKey = "standardDate";
    private final String adminIdKey = "adminId";

    public KPgReconcileJobParameters() {
    }

    public KPgReconcileJobParameters(LocalDate standardDate, String adminId) {
	    this.standardDate = standardDate;
	    this.adminId = adminId;
    }

    public KPgReconcileJobParameters(String standardDate, String adminId) {
        this(LocalDate.parse(standardDate, DATE_FORMATTER), adminId);
    }

    public KPgReconcileJobParameters(String adminId) {
        this(LocalDate.now().minusDays(1), adminId);
    }

    @Override
    public JobParameters toJobParameter() {
        return new JobParametersBuilder()
                .addString(standardDateKey, standardDate.format(DATE_FORMATTER))
                .addString(adminIdKey, adminId)
                .toJobParameters();
    }

    @Override
    public void validate() throws IllegalArgumentException {

        if (standardDate == null) {
            throw new IllegalArgumentException("standardDate is blank");
        }

        if (StringUtils.isBlank(adminId)) {
            throw new IllegalArgumentException("adminId is blank");
        }

    }

    public void setStandardDate(String standardDate) {
        try {
            this.standardDate = LocalDate.parse(standardDate, DATE_FORMATTER);
        } catch (Exception e) {
            throw new IllegalArgumentException("standardDate is invalid format");
        }
    }

    public void setAdminId(String adminId) {
        this.adminId = adminId;
    }

    @Override
    public List<JobParametersDetailDto> getJobParametersDetailList() {
        List<JobParametersDetailDto> list = new ArrayList<>();

        JobParametersDetailDto standardDate = new JobParametersDetailDto(standardDateKey, "기준일자", "YYYY-MM-DD 형식");
        JobParametersDetailDto adminId = new JobParametersDetailDto(adminIdKey, "배치실행 관리자ID", "");

        list.add(standardDate);
        list.add(adminId);

        return list;
    }
}
