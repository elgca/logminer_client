package elgca.logmnr;

public class LogMinerException extends RuntimeException {
    public LogMinerException(String message, Throwable cause) {
        super(message, cause);
    }

    public LogMinerException(String message) {
        super(message);
    }

    public LogMinerException(Throwable cause) {
        super(cause);
    }
}
