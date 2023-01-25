package kr.co.pg.util;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class WebClientTemplate {

    private WebClient webClient;

    public WebClientTemplate(WebClient.Builder builder) {
        webClient = builder.build();
    }

    public Map<String, Object> postRequestApi(String url, Object body, Map<String, Object> headerInfo) {
        return webClient
                .post()
                .uri(url)
                .headers(httpHeaders -> {
                    for (String key : headerInfo.keySet()) {
                        httpHeaders.add(key, headerInfo.get(key).toString());
                    }
                })
                .body(BodyInserters.fromValue(new Gson().toJson(body)))
                .exchangeToMono(clientResponse -> {
                    if (clientResponse.statusCode().is2xxSuccessful()) return clientResponse.bodyToMono(Map.class);
                    return Mono.error(new RuntimeException("Http Status : " + clientResponse.statusCode()));
                }).timeout(Duration.ofSeconds(15))
                .blockOptional().orElse(new HashMap());
    }
}
