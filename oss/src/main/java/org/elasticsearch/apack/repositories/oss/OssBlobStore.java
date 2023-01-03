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
