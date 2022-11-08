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


import com.aliyun.oss.OSSException;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import java.io.InputStream;
import java.util.Map;

public class OssBlobStore implements BlobStore {

    private final OssService ossService;

    public OssBlobStore(OssService ossService) {
        this.ossService = ossService;
    }

    @Override
    public BlobContainer blobContainer(BlobPath blobPath) {
        return new OssBlobContainer(this, blobPath);
    }

    @Override
    public void delete(BlobPath path) {
        delete(path.buildAsString());
    }

    public boolean blobExists(String blob) {
        return ossService.exists(blob);
    }

    public void delete(String blob) throws OSSException {
        ossService.delete(blob);
    }

    public InputStream getInputStream(String blob) throws OSSException {
        return ossService.getInputStream(blob);
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws OSSException {
        ossService.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    public Map<String, BlobMetaData> listBlobsByPrefix(String path, String prefix) {
        return ossService.listBlobsByPrefix(path, prefix);
    }

    @Override
    public void close() {
    }

}
