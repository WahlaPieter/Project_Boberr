package uantwerpen.be.fti.ei.Project.NamingServer;

public class HashingUtil {

    public static int generateHash(String input) {
        int hash = input.hashCode();
        hash = Math.abs(hash) % 32768;
        return hash;
    }
}
