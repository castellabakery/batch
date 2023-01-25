package kr.co.pg.util;

import com.tnc.lib.cipher.util.CipherUtil;

public class TEncrypt {
    public static void kEncryptInit(){
        String cipherType = "";
        String domain = "";
        String dbAes1 = "";
        String KMSType = "";
        String KMSPath = "";
        String KMSKey = "";

        CipherUtil.init(cipherType, domain, dbAes1, KMSType, KMSPath, KMSKey);
    }
    public static String kEncAesEncrypt(String text){
        kEncryptInit();
        return CipherUtil.encAes(text);
    }
    public static String kSha512Encrypt(String text){
        kEncryptInit();
        return CipherUtil.getSha512(text);
    }
}
