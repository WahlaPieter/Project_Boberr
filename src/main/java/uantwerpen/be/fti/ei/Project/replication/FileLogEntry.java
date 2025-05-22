package uantwerpen.be.fti.ei.Project.replication;

import java.util.HashSet;
import java.util.Set;

public class FileLogEntry {
    private String owner;
    private Set<String> downloadLocations = new HashSet<>();

    public FileLogEntry(String owner) {
        this.owner = owner;
    }

    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    public Set<String> getDownloadLocations() { return downloadLocations; }

    public void addDownloadLocation(String ip) { downloadLocations.add(ip); }
    public void removeDownloadLocation(String ip) { downloadLocations.remove(ip); }
}
