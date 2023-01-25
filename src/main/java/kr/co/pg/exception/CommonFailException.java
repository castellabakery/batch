package kr.co.pg.exception;

import lombok.Getter;

@Getter
public class CommonFailException extends RuntimeException {
    private CommonErrorCodeEnum errorCodeEnum;

    public CommonFailException(String errorCode) {
        super(errorCode);
        this.errorCodeEnum = CommonErrorCodeEnum.getEnum(errorCode);
    }
}
