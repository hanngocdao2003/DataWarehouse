package log;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;
import java.util.UUID;

public class MailTo {
    static final String username = "cromeecs@gmail.com";
    static final String password = "flspqunfbuaenlob";

    public static String generateToken() {
        return UUID.randomUUID().toString();
    }

    public static boolean sendVerificationEmail(String mes) {

        String toMail = "21130345@st.hcmuaf.edu.vn";

        Properties properties = new Properties();
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.host", "smtp.gmail.com");
        properties.put("mail.smtp.port", "587");
        properties.put("mail.smtp.ssl.trust", "*");


        Session session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(username));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toMail));
            message.setSubject("Thông báo dữ liệu");
            message.setText(mes);

            Transport.send(message);

            System.out.println("Email sent successfully.");
            return true;

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

//    public static void main(String[] args) {
//        System.out.println(sendVerificationEmail("hanngocdao2003@gmail.com"));
//    }
}
