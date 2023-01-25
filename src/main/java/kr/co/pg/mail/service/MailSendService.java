package kr.co.pg.mail.service;

import kr.co.pg.mail.config.Hmac;
import kr.co.pg.mail.domain.entity.AlarmEntity;
import kr.co.pg.mail.enumcode.MailTemplateEnum;
import kr.co.pg.mail.repository.AlarmRepository;
import kr.co.pg.util.WebClientTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class MailSendService {

    private static final String SENDER_MAIL_ADDRESS = "";
    private static final String CDN_PATH = "";
    private static final String MAIL_TEMPLATE_PATH = "/static/html/";
    private static final String SENDER_ADDRESS = "error@co.kr";
    private static final String SENDER_NAME = "ERROR";
    private final WebClientTemplate webClientTemplate;
    private final AlarmRepository alarmRepository;

    @Autowired
    public MailSendService(AlarmRepository alarmRepository
            , WebClientTemplate webClientTemplate) {
        this.alarmRepository = alarmRepository;
        this.webClientTemplate = webClientTemplate;
    }

    public void sendEmail(String title, String contents) throws Exception {
        log.info("sendEmail start");

        List<String> recipientList =
                alarmRepository.findByUseYn("Y")
                        .stream()
                        .map(AlarmEntity::getEmail)
                        .collect(Collectors.toList());

        if (recipientList.size() > 0) {
            sendEmail(recipientList, title, getHtmlAlarmHTemplate(contents));
        }
    }

    /**
     * 네이버 클라우드 메일 전송
     */
    private void sendEmail(List<String> recipientList, String title, String content) throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("senderAddress", SENDER_ADDRESS);
        data.put("senderName", SENDER_NAME);
        data.put("title", title);
        data.put("body", content);
        data.put("unsubscribeMessage", "false");
        data.put("individual", "true");

        List<Map<String, Object>> recipients = new ArrayList<>();
        for (String recipient : recipientList) {
            Map<String, Object> recipientMap = new HashMap<>();
            recipientMap.put("address", recipient);
            recipientMap.put("name", recipient);
            recipientMap.put("type", "R");
            recipients.add(recipientMap);
        }

        Map<String, Object> parameters = new HashMap<>();
        data.put("parameters", parameters);
        data.put("recipients", recipients);

        try {
            Map<String, Object> response = webClientTemplate.postRequestApi(SENDER_MAIL_ADDRESS, data, Hmac.createHmacSHA256AndNaverApiHeaderInfo());
            if (response.containsKey("requestId")) log.info("sendEmail end");
        } catch (Exception e) {
            throw e;
        }
    }

    private String getHtmlAlarmHTemplate(String contents) throws Exception {
        try {
            String content = StreamUtils.copyToString(MailTemplateEnum.class.getResourceAsStream(MAIL_TEMPLATE_PATH + MailTemplateEnum.ERROR.getTemplateType()), Charset.forName("UTF-8"));
            content = content.replace("${imagePath}", CDN_PATH);
            content = content.replace("${contents}", contents);
            return content;
        } catch (Exception e) {
            log.error("getHtmlAlarmHTemplate error {}", e.getMessage());
            throw e;
        }
    }
}
