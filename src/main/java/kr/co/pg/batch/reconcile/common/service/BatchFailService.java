package kr.co.pg.batch.reconcile.common.service;

import kr.co.pg.mail.service.MailSendService;
import kr.co.pg.scheduler.domain.SchedulerJobParameters;
import kr.co.pg.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class BatchFailService {

    private MailSendService mailSendService;

    private final String HTML_NEW_LINE = "<br>";

    @Value("${spring.config.activate.on-profile}")
    private String OPERATING_ENVIRONMENT;

    @Autowired
    public BatchFailService(MailSendService mailSendService) {
        this.mailSendService = mailSendService;
    }

    public void execute(String jobName, SchedulerJobParameters schedulerJobParameters, String errorMessage, String exceptionMessage) {

        boolean isExternal = schedulerJobParameters.isExternalJobParameter();
        Map<String, JobParameter> parameterMap = schedulerJobParameters.getJobParameters().getParameters();


        String title = new StringBuilder("[Auto Batch-").append(OPERATING_ENVIRONMENT).append("] ").append(jobName).append(" ").append("실패").toString();
        StringBuilder contentBuilder = new StringBuilder("[").append(jobName).append("-").append(OPERATING_ENVIRONMENT).append("] ").append("배치 실패 상세")
                .append(HTML_NEW_LINE).append("호출경로: ").append(isExternal ? "백오피스 관리자" : "오토배치 시스템").append(HTML_NEW_LINE);

        for (String key : parameterMap.keySet()) {
            contentBuilder.append(key).append(": ").append(parameterMap.get(key)).append(HTML_NEW_LINE);
        }

        contentBuilder.append("errorMessage: ").append(replaceAllNewLine(errorMessage)).append(HTML_NEW_LINE);
        contentBuilder.append("exceptionMessage: ").append(replaceAllNewLine(exceptionMessage)).append(HTML_NEW_LINE);

        String content = contentBuilder.toString();
        try {
            mailSendService.sendEmail(title, content);
        } catch (Exception e) {
            log.error("Failed to send email for batch fail - title: {}, content: {}, reason: {}", title, content, e.getMessage());
        }

    }

    private String replaceAllNewLine(String org) {

        if (StringUtils.isBlank(org)) {
            return org;
        }

        return org.replaceAll("\n", HTML_NEW_LINE);
    }
}
