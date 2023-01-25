package kr.co.pg.mail.config;

import org.apache.tomcat.util.codec.binary.Base64;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class Hmac {

    private final static String ACCESS_KEY = "";  // access key id (from portal or sub account)
    private final static String SECRET_KEY = "";

    public static Map<String, Object> createHmacSHA256AndNaverApiHeaderInfo() throws Exception {
        String space = " ";  // 공백
        String newLine = "\n";  // 줄바꿈
        String method = "POST";  // HTTP 메소드
        String url = "";  // 도메인을 제외한 "/" 아래 전체 url (쿼리스트링 포함)
        String timestamp = String.valueOf(Instant.now().toEpochMilli());  // 현재 타임스탬프 (epoch, millisecond)
        String accessKey = ACCESS_KEY;  // access key id (from portal or sub account)
        String secretKey = SECRET_KEY;  // secret key (from portal or sub account)

        String message = new StringBuilder()
                .append(method)
                .append(space)
                .append(url)
                .append(newLine)
                .append(timestamp)
                .append(newLine)
                .append(accessKey)
                .toString();

        SecretKeySpec signingKey = new SecretKeySpec(secretKey.getBytes("UTF-8"), "HmacSHA256");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(signingKey);

        byte[] rawHmac = mac.doFinal(message.getBytes("UTF-8"));

        Map<String, Object> response = new HashMap<>();
        response.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        response.put("x-ncp-apigw-timestamp", timestamp);
        response.put("x-ncp-iam-access-key", accessKey);
        response.put("x-ncp-apigw-signature-v2", Base64.encodeBase64String(rawHmac));
        return response;
    }
}
