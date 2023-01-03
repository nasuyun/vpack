package org.elasticsearch.apack.repositories.oss;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Map;

public class OssBlobContainer extends AbstractBlobContainer {

    private final Logger logger = LogManager.getLogger(OssBlobContainer.class);

    private final OssBlobStore blobStore;
    private final String keyPath;

    /**
     * OssBlobContainer
     *
     * @param blobStore
     * @param blobPath
     */
    public OssBlobContainer(OssBlobStore blobStore, BlobPath blobPath) {
        super(blobPath);
        this.blobStore = blobStore;
        this.keyPath = blobPath.buildAsString();
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        try {
            return blobStore.listBlobsByPrefix(keyPath, blobNamePrefix);
        } catch (OSSException e) {
            logger.warn("can not access [{}] in container {{}}: {}", blobNamePrefix, blobStore, e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        try {
            String deleteKey = buildKey(blobName);
            blobStore.delete(deleteKey);
        } catch (OSSException e) {
            if (e.getErrorCode().equals(OSSErrorCode.NO_SUCH_BUCKET)
                    || e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                throw new NoSuchFileException(e.getMessage());
            }
            throw new IOException(e);
        }
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            return blobStore.blobExists(buildKey(blobName));
        } catch (OSSException e) {
            if (e.getErrorCode().equals(OSSErrorCode.NO_SUCH_BUCKET)
                    || e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                return false;
            }
        }
        return false;
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        if (blobExists(blobName) == false) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        try {
            return blobStore.getInputStream(buildKey(blobName));
        } catch (OSSException e) {
            if (e.getErrorCode().equals(OSSErrorCode.NO_SUCH_BUCKET)
                    || e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                throw new NoSuchFileException(e.getMessage());
            }
            throw new IOException(e);
        }
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        try {
            blobStore.writeBlob(buildKey(blobName), inputStream, blobSize, failIfAlreadyExists);
        } catch (OSSException e) {
            throw new IOException("Could not write blob " + blobName, e);
        }
    }

    protected String buildKey(String blobName) {
        return keyPath + (blobName == null ? "" : blobName);
    }
}
