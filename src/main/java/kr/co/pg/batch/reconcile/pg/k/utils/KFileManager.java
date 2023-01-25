package kr.co.pg.batch.reconcile.pg.k.utils;

import kr.co.pg.batch.reconcile.pg.k.domain.KPgShopIdAndKeySet;
import kr.co.pg.util.DateFormatUtils;
import kr.co.pg.util.StringUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Getter
@Component
public class KFileManager {

    public static final String SUPPORT_FILE_TYPE_PO = "";
    public static final String SUPPORT_FILE_TYPE_PT = "";

    public static final String DEFAULT_EXTENSION = "txt";

    @Value("${k.pg.filedownload.host}")
    private String host;
    @Value("${k.pg.filedownload.port}")
    private int port;
    @Value("${k.pg.filedownload.download-type}")
    private String downloadType;
    @Value("${k.pg.filedownload.shop-ids}")
    private String shopIds;
    @Value("${k.pg.filedownload.keys}")
    private String keys;
    @Value("${k.pg.filedownload.origin-path}")
    private String originPath;

    private List<KPgShopIdAndKeySet> kPgShopIdAndKeySetList = new ArrayList<>();

    @PostConstruct
    public void init() throws Exception {

        String[] shopIdArray = shopIds.split(",");
        String[] keyArray = keys.split(",");

        if (shopIdArray.length != keyArray.length) {
            throw new Exception("k shop id and key must be the same length");
        }

        for (int i = 0; i < shopIdArray.length; i++) {
            StringUtils.notBlank(shopIdArray[i], "k shop id must not be null");
            StringUtils.notBlank(keyArray[i], "k Key must not be null");
            kPgShopIdAndKeySetList.add(new KPgShopIdAndKeySet(shopIdArray[i], keyArray[i]));
        }

        log.info("===== kPgShopIdAndKeySetList - size: {} =====", kPgShopIdAndKeySetList.size());
        for (KPgShopIdAndKeySet kPgShopIdAndKeySet : kPgShopIdAndKeySetList) {
            log.info("{}", kPgShopIdAndKeySet);
        }
    }

    public String getReconcileFileFullPath(String fileType, LocalDate fileDate, String shopId) {
        String fileName = getFileName(fileType, fileDate, shopId);
        String fullPath = new StringBuilder(originPath)
                .append(fileName)
                .append(".").append(DEFAULT_EXTENSION)
                .toString();
        return fullPath;
    }

    public String getFileName(String fileType, LocalDate fileDate, String shopId) {
        return new StringBuilder().append(fileType).append("_").append(shopId).append("_").append(fileDate.format(DateFormatUtils.FULL_DATE)).toString();
    }

}
