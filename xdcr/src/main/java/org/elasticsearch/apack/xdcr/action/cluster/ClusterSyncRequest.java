package org.elasticsearch.apack.xdcr.action.cluster;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.apack.xdcr.task.TaskIds;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.apack.xdcr.utils.Actions;

import java.io.IOException;

public class ClusterSyncRequest extends AcknowledgedRequest<ClusterSyncRequest> {

    private String repository;

    private String includes;

    private String excludes;

    public ClusterSyncRequest() {
    }

    public ClusterSyncRequest(String repository) {
        this.repository = repository;
        this.includes = "";
        this.excludes = "";
    }

    public ClusterSyncRequest(String repository, String includes, String excludes) {
        this.repository = repository;
        this.includes = includes;
        this.excludes = excludes;
    }

    public String repository() {
        return repository;
    }

    public String inclouds() {
        return includes;
    }

    public String excludes() {
        return excludes;
    }

    public String getTaskId() {
        return TaskIds.syncClusterTaskName(repository);
    }

    @Override
    public ActionRequestValidationException validate() {
        return Actions.notNull("repository", repository);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        repository = in.readString();
        includes = in.readString();
        excludes = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(includes);
        out.writeString(excludes);
    }

    /**
     * RequestBuilder
     */
    public static class Builder extends MasterNodeOperationRequestBuilder<ClusterSyncRequest, AcknowledgedResponse, Builder> {

        protected Builder(ElasticsearchClient client, Action<ClusterSyncRequest, AcknowledgedResponse, Builder> action, ClusterSyncRequest request) {
            super(client, action, request);
        }
    }

}
