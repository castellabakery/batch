package kr.co.pg.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringUtils {

    private StringUtils() {
    }

    public static boolean isBlank(String value) {

        if (org.apache.commons.lang3.StringUtils.isEmpty(value)) { // null, blank
            return true;
        }

        if (org.apache.commons.lang3.StringUtils.isBlank(value)) { // white space
            return true;
        }

        if ("null".equals(value)) {
            return true;
        }

        return false;
    }

    public static void notBlank(String value, String message) {
        if (isBlank(value)) {
            throw new IllegalArgumentException(message);
        }
    }
}
