package kr.co.pg.mail.enumcode;

import lombok.Getter;

@Getter
public enum MailTemplateEnum {
    ERROR("errorAlarm.html");

    private String templateType;

    MailTemplateEnum(String templateType) {
        this.templateType = templateType;
    }
}
