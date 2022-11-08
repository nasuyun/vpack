package org.elasticsearch.apack.repositories.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;

import java.io.InputStream;
import java.util.Map;

import static org.elasticsearch.apack.repositories.oss.SocketAccess.doPrivilegedIOException;
import static org.elasticsearch.apack.repositories.oss.SocketAccess.doPrivilegedVoidException;

public class OssService {

    private final String bucketName;
    private final String endpoint;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final String basePath;

    public OssService(Settings settings) {
        bucketName = settings.get("bucket_name");
        endpoint = settings.get("endpoint");
        accessKeyId = settings.get("access_key_id");
        accessKeySecret = settings.get("access_key_secret");
        basePath = settings.get("base_path");
    }

    private OSS client() {
        return new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
    }

    public boolean exists(String key) {
        Boolean existed = doPrivilegedIOException(
                () -> {
                    try {
                        return client().doesObjectExist(bucketName, prefixRoot(key));
                    } catch (OSSException e) {
                        if (e.getMessage().contains("NoSuchKey")
                                || e.getErrorCode().equals(OSSErrorCode.NO_SUCH_BUCKET)
                                || e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
                            return false;
                        }
                        throw e;
                    }
                }
        );
        return existed;
    }

    /**
     * OSS 不具备删除目录功能
     * 如果为Object 则直接删除
     * 如果为目录则删除所有子Object
     *
     * @param key keyOrPath
     */
    public void delete(String key) {
        String actualKey = prefixRoot(key);
        doPrivilegedVoidException(
                () -> {
                    if (exists(actualKey)) {
                        client().deleteObject(bucketName, actualKey);
                    } else {
                        ObjectListing objectListing = client().listObjects(bucketName, actualKey);
                        if (objectListing != null) {
                            for (OSSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                                client().deleteObject(bucketName, objectSummary.getKey());
                            }
                        }
                    }
                }
        );
    }

    public InputStream getInputStream(String key) {
        String actualKey = prefixRoot(key);
        return doPrivilegedIOException(
                () -> {
                    // TODO exist throw NoSuchFileException
                    OSSObject ossObject = client().getObject(bucketName, actualKey);
                    return ossObject.getObjectContent();
                }
        );
    }

    public void writeBlob(String key, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
        doPrivilegedIOException(
                () -> client().putObject(bucketName, prefixRoot(key), inputStream)
        );
    }

    public Map<String, BlobMetaData> listBlobsByPrefix(String path, String prefix) {
        final String pathPrefix = path + (prefix == null ? "" : prefix);
        final MapBuilder<String, BlobMetaData> mapBuilder = MapBuilder.newMapBuilder();
        doPrivilegedVoidException(
                () -> {
                    ObjectListing objectListing = client().listObjects(bucketName, prefixRoot(pathPrefix));
                    for (OSSObjectSummary blob : objectListing.getObjectSummaries()) {
                        final String suffixName = blob.getKey().substring(prefixRoot(path).length());
                        mapBuilder.put(suffixName, new PlainBlobMetaData(suffixName, blob.getSize()));
                    }
                });
        return mapBuilder.immutableMap();
    }

    private String prefixRoot(String key) {
        return this.basePath + "/" + key;
    }

}
