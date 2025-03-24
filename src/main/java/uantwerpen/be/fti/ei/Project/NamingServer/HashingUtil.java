package uantwerpen.be.fti.ei.Project.NamingServer;

public class HashingUtil {

    public static int generateHash(String input) {
        int max = Integer.MAX_VALUE;
        int min = Integer.MIN_VALUE;
        return (input.hashCode() + max) * (32768 / (max + Math.abs(min)));
    }
}
