package org.elasticsearch.apack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class RetentionLeases {

    public static final Setting<TimeValue> RETENTION_LEASE_RENEW_INTERVAL_SETTING =
            Setting.timeSetting(
                    "index.xdcr.retention_lease.renew_interval",
                    new TimeValue(30, TimeUnit.SECONDS),
                    new TimeValue(0, TimeUnit.MILLISECONDS),
                    Setting.Property.NodeScope);

    public static String retentionLeaseId(
            final String localClusterName,
            final Index followerIndex,
            final String remoteClusterAlias,
            final Index leaderIndex) {
        return String.format(
                Locale.ROOT,
                "%s/%s/%s-following-%s/%s/%s",
                localClusterName,
                followerIndex.getName(),
                followerIndex.getUUID(),
                remoteClusterAlias,
                leaderIndex.getName(),
                leaderIndex.getUUID());
    }

    public static Optional<RetentionLeaseAlreadyExistsException> syncAddRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final long retainingSequenceNumber,
            final Client remoteClient,
            final TimeValue timeout) {
        try {
            final PlainActionFuture<RetentionLeaseActions.Response> response = new PlainActionFuture<>();
            asyncAddRetentionLease(leaderShardId, retentionLeaseId, retainingSequenceNumber, remoteClient, response);
            response.actionGet(timeout);
            return Optional.empty();
        } catch (final RetentionLeaseAlreadyExistsException e) {
            return Optional.of(e);
        }
    }

    public static void asyncAddRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final long retainingSequenceNumber,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.AddRequest request =
                new RetentionLeaseActions.AddRequest(leaderShardId, retentionLeaseId, retainingSequenceNumber, "xdcr");
        remoteClient.execute(RetentionLeaseActions.Add.INSTANCE, request, listener);
    }

    public static Optional<RetentionLeaseNotFoundException> syncRenewRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final long retainingSequenceNumber,
            final Client remoteClient,
            final TimeValue timeout) {
        try {
            final PlainActionFuture<RetentionLeaseActions.Response> response = new PlainActionFuture<>();
            asyncRenewRetentionLease(leaderShardId, retentionLeaseId, retainingSequenceNumber, remoteClient, response);
            response.actionGet(timeout);
            return Optional.empty();
        } catch (final RetentionLeaseNotFoundException e) {
            return Optional.of(e);
        }
    }

    public static void asyncRenewRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final long retainingSequenceNumber,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.RenewRequest request =
                new RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, retainingSequenceNumber, "xdcr");
        remoteClient.execute(RetentionLeaseActions.Renew.INSTANCE, request, listener);
    }

    public static void asyncRemoveRetentionLease(
            final ShardId leaderShardId,
            final String retentionLeaseId,
            final Client remoteClient,
            final ActionListener<RetentionLeaseActions.Response> listener) {
        final RetentionLeaseActions.RemoveRequest request = new RetentionLeaseActions.RemoveRequest(leaderShardId, retentionLeaseId);
        remoteClient.execute(RetentionLeaseActions.Remove.INSTANCE, request, listener);
    }

}
