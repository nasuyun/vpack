/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
