package uantwerpen.be.fti.ei.Project.Agents;

import java.io.Serializable;

/**
 * Simpele DTO voor lock-aanvragen.
 */
public class LockRequest implements Serializable {
    private String filename;
    private String requesterIp;

    public LockRequest() {}

    public LockRequest(String filename, String requesterIp) {
        this.filename = filename;
        this.requesterIp = requesterIp;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getRequesterIp() {
        return requesterIp;
    }

    public void setRequesterIp(String requesterIp) {
        this.requesterIp = requesterIp;
    }
}
