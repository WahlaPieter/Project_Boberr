package uantwerpen.be.fti.ei.Project.replication;

public class FileTransferRequest {
    public String fileName;
    public byte[] data;

    public FileTransferRequest() {} // JSON (de)serialisation (convert string -> object)

    public FileTransferRequest(String fileName, byte[] data) {
        this.fileName = fileName;
        this.data = data;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
