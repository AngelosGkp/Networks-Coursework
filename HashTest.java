import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

public class HashTest {
    public static void main(String[] args) throws Exception {
        String[] keys = {
            "D:jabberwocky0","D:jabberwocky1","D:jabberwocky2",
            "D:jabberwocky3","D:jabberwocky4","D:jabberwocky5","D:jabberwocky6"
        };
        for (String key : keys) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(key.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            System.out.println(key + " -> " + sb.toString().substring(0, 8) + "...");
        }
    }
}