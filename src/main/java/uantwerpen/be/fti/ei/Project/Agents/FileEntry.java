package uantwerpen.be.fti.ei.Project.Agents;

import java.io.Serializable;

/**
 * Class representing a file in the distributed system
 * Used by agents to synchronise or transfer information about files
 */
public class FileEntry implements Serializable {
    private String filename;
    private boolean isLocked;
    private String ownerIp;

    // Constructor
    public FileEntry(String filename, boolean isLocked, String ownerIp) {
        this.filename = filename;
        this.isLocked = isLocked;
        this.ownerIp = ownerIp;
    }

    // Getters
    public String getFilename() {
        return filename;
    }

    public boolean isLocked() {
        return isLocked;
    }

    public String getOwnerIp() {
        return ownerIp;
    }

    // Setters
    public void setLocked(boolean locked) {
        isLocked = locked;
    }

    public void setOwnerIp(String ownerIp) {
        this.ownerIp = ownerIp;
    }

    @Override
    public String toString() {
        return String.format("FileEntry{filename='%s', locked=%b, owner='%s'}", filename, isLocked, ownerIp);
    }
}
