package uantwerpen.be.fti.ei.Project.Agents;

import java.io.Serializable;


public class LockRequest implements Serializable {
    private String filename;
    private String requesterIp;
    private String action; // "LOCK" of "UNLOCK"

    public LockRequest() {}

    public LockRequest(String filename, String requesterIp, String action) {
        this.filename = filename;
        this.requesterIp = requesterIp;
        this.action = action;
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
public String getAction() {
    return action;
}

public void setAction(String action) {
    this.action = action;
}
}
