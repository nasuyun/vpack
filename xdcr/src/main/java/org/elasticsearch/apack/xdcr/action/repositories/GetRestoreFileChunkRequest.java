package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteClusterAwareRequest;

import java.io.IOException;

public class GetRestoreFileChunkRequest extends ActionRequest implements RemoteClusterAwareRequest {

    private DiscoveryNode node;
    private String sessionUUID;
    private String fileName;
    private int size;

    GetRestoreFileChunkRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public GetRestoreFileChunkRequest(DiscoveryNode node, String sessionUUID, String fileName, int size) {
        this.node = node;
        this.sessionUUID = sessionUUID;
        this.fileName = fileName;
        this.size = size;
        assert size > -1 : "The file chunk request size must be positive. Found: [" + size + "].";
    }

    GetRestoreFileChunkRequest(StreamInput in) throws IOException {
        super(in);
        node = null;
        sessionUUID = in.readString();
        fileName = in.readString();
        size = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
        out.writeString(fileName);
        out.writeVInt(size);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        node = null;
        sessionUUID = in.readString();
        fileName = in.readString();
        size = in.readVInt();
    }

    String getSessionUUID() {
        return sessionUUID;
    }

    String getFileName() {
        return fileName;
    }

    int getSize() {
        return size;
    }

    @Override
    public DiscoveryNode getPreferredTargetNode() {
        assert node != null : "Target node is null";
        return node;
    }
}
