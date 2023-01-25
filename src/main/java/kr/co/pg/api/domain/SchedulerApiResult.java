package kr.co.pg.api.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchedulerApiResult<T> {

    private final static int SUCCESS = 1000;
    private final static String SUCCESS_MESSAGE = "성공";

    public final static int FAIL = -1000;


    private final T data;
    private final int code;
    private final String message;

    private SchedulerApiResult(T data, int code, String message) {
        this.data = data;
        this.code = code;
        this.message = message;
    }

    public static <T> SchedulerApiResult<T> success() {
        return new SchedulerApiResult(null, SUCCESS, SUCCESS_MESSAGE);
    }

    public static <T> SchedulerApiResult<T> success(T data) {
        return new SchedulerApiResult(data, SUCCESS, SUCCESS_MESSAGE);
    }

    public static <T> SchedulerApiResult<T> fail(String message) {
        return new SchedulerApiResult(null, FAIL, message);
    }

    public static <T> SchedulerApiResult<T> fail(int code, String message) {
        return new SchedulerApiResult(null, code, message);
    }

    public static <T> SchedulerApiResult<T> fail(T data, int code, String message) {
        return new SchedulerApiResult(data, code, message);
    }
}
