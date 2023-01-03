package org.elasticsearch.apack.repositories.oss;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

public class OssRepository extends BlobStoreRepository {

    public static final String TYPE = "oss";

    private final BlobPath basePath;
    private final OssService ossService;
    private ByteSizeValue chunkSize;
    private boolean isCompress;
    private boolean isReadOnly;

    public OssRepository(
            RepositoryMetaData metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry
    ) {
        super(metadata, environment.settings(), namedXContentRegistry);
        Settings settings = metadata.settings();
        this.basePath = BlobPath.cleanPath();
        this.ossService = new OssService(settings);
        this.chunkSize = settings.getAsBytesSize("chunk_size", new ByteSizeValue(40, ByteSizeUnit.MB));
        this.isCompress = settings.getAsBoolean("compress", true);
        this.isReadOnly = settings.getAsBoolean("read_only", false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected OssBlobStore createBlobStore() {
        final OssBlobStore blobStore = new OssBlobStore(ossService);
        return blobStore;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    void setChunkSize(ByteSizeValue chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    protected boolean isCompress() {
        return isCompress;
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

}
