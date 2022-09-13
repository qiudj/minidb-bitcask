package abc.xyz.exception;

public class MiniDbException extends Exception{

    public MiniDbException(String message) {
        super(message);
    }

    public MiniDbException(MiniDBExceptionType type){
        super("[" + type.getCode() + "] " + type.getDesc() +   ".");
    }

    public MiniDbException(MiniDBExceptionType type, Throwable e) {
        super("[" + type.getCode() + "] " + type.getDesc() +   ", caused by => " + e.toString());
        this.initCause(e);
    }

    public MiniDbException(Throwable throwable) {
        super(throwable);
    }
}
