package kr.co.pg.util;

import java.time.format.DateTimeFormatter;

public class DateFormatUtils {

    public static final DateTimeFormatter FULL_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static final DateTimeFormatter FULL_TIME = DateTimeFormatter.ofPattern("HHmmss");

    public static final DateTimeFormatter DASH_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final DateTimeFormatter COLON_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final DateTimeFormatter DASH_YEAR_MONTH = DateTimeFormatter.ofPattern("yyyy-MM");
}
