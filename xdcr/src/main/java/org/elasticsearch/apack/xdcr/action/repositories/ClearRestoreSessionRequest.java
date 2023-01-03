package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.RemoteClusterAwareRequest;

import java.io.IOException;

public class ClearRestoreSessionRequest extends ActionRequest implements RemoteClusterAwareRequest {

    private DiscoveryNode node;
    private String sessionUUID;

    ClearRestoreSessionRequest() {
    }

    public ClearRestoreSessionRequest(String sessionUUID, DiscoveryNode node) {
        this.sessionUUID = sessionUUID;
        this.node = node;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sessionUUID = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
    }

    String getSessionUUID() {
        return sessionUUID;
    }

    @Override
    public DiscoveryNode getPreferredTargetNode() {
        return node;
    }
}
