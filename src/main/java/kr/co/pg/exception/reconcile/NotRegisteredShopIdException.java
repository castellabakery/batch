package kr.co.pg.exception.reconcile;


public class NotRegisteredShopIdException extends RuntimeException {

    public NotRegisteredShopIdException(String errorCode) {
        super(errorCode);
    }
}
