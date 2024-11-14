package Email;

public interface IJavaMail {
    boolean send(String to,String subject,String message);
}
