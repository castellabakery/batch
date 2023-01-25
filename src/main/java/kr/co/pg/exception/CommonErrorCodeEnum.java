package kr.co.pg.exception;

import lombok.Getter;

@Getter
public enum CommonErrorCodeEnum {
    ERROR_DATABASE_001("ERROR_DATABASE_001", "DATABASE", "조회된 데이터가 없습니다."),
    ERROR_SYSTEM_FAIL("ERROR_SYSTEM_FAIL", "SYSTEM", "시스템 에러, 예외를 처리할 수 없습니다.");
    private String code;
    private String message;
    private String type;

    CommonErrorCodeEnum(String code, String type, String message) {
        this.code = code;
        this.type = type;
        this.message = message;
    }

    public static CommonErrorCodeEnum getEnum(String errorCode) {
        for (CommonErrorCodeEnum errors : CommonErrorCodeEnum.values()) {
            if (errors.code.equals(errorCode)) {
                return errors;
            }
        }
        return CommonErrorCodeEnum.ERROR_SYSTEM_FAIL;
    }

    public static boolean isExist(String errorCode) {
        for (CommonErrorCodeEnum errors : CommonErrorCodeEnum.values()) {
            if (errors.code.equals(errorCode)) {
                return true;
            }
        }
        return false;
    }
}
