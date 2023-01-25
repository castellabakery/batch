package kr.co.pg.util;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class KEncrypt {

    private KEncrypt() {
    }

    private final static String SECRET_ALGORITHM = "";
    private final static String SECRET_TRANSFORMATION = "";
    private final static String SECRET_KEY = "";
    private final static String SECRET_IV = "";
    private final static String SECRET_CHARSET = "";

    private final static int SECRET_KEY_BYTE_SIZE = 16;

    public static String encrypt(String message) throws Exception {
        SecretKey secretKey = new SecretKeySpec(SECRET_KEY.substring(0, SECRET_KEY_BYTE_SIZE).getBytes(), SECRET_ALGORITHM);

        Cipher c = Cipher.getInstance(SECRET_TRANSFORMATION);
        String iv = SECRET_IV.substring(0, 16);
        c.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv.getBytes()));

        byte[] encrypted = c.doFinal(message.getBytes(SECRET_CHARSET));
        return new String(Base64.encodeBase64URLSafe(encrypted));
    }

    public static String encryptWithoutCatch(String message) {
        SecretKey secretKey = new SecretKeySpec(SECRET_KEY.substring(0, SECRET_KEY_BYTE_SIZE).getBytes(), SECRET_ALGORITHM);
        String iv = SECRET_IV.substring(0, 16);

        try {
            Cipher c = Cipher.getInstance(SECRET_TRANSFORMATION);
            c.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv.getBytes()));
            byte[] encrypted = c.doFinal(message.getBytes(SECRET_CHARSET));
            return new String(Base64.encodeBase64URLSafe(encrypted));
        } catch (Exception e) {
            e.printStackTrace();
            return message;
        }
    }

    public static String decrypt(String message) throws Exception {
        SecretKey secretKey = new SecretKeySpec(SECRET_KEY.substring(0, SECRET_KEY_BYTE_SIZE).getBytes(), SECRET_ALGORITHM);
        Cipher c = Cipher.getInstance(SECRET_TRANSFORMATION);
        String iv = SECRET_IV.substring(0, 16);
        c.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv.getBytes(SECRET_CHARSET)));
        byte[] byteStr = Base64.decodeBase64(message.getBytes());
        return new String(c.doFinal(byteStr), SECRET_CHARSET);
    }

    public static String decryptWithoutCatch(String message) {
        SecretKey secretKey = new SecretKeySpec(SECRET_KEY.substring(0, SECRET_KEY_BYTE_SIZE).getBytes(), SECRET_ALGORITHM);
        String iv = SECRET_IV.substring(0, 16);
        try {
            Cipher c = Cipher.getInstance(SECRET_TRANSFORMATION);
            c.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv.getBytes(SECRET_CHARSET)));
            byte[] byteStr = Base64.decodeBase64(message.getBytes());
            return new String(c.doFinal(byteStr), SECRET_CHARSET);
        } catch (Exception e) {
            e.printStackTrace();
            return message;
        }
    }

}