package uantwerpen.be.fti.ei.Project.NamingServer;

public class HashingUtil {

    public static int generateHash(String input) {
        int hash = input.hashCode();
        hash = hash & Integer.MAX_VALUE; // Ensures non-negative
        return hash % 32768; // 0-32767
    }
}
