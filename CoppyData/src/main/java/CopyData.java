import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.AccountSasSignatureValues;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class CopyData {
//    public static final String CONNECTION_STRING =
//            "DefaultEndpointsProtocol=https;" +
//                    "AccountName=flinkstr01;" +
//                    "AccountKey=fzAnZsiM0+YSM4hm2eo0QZo5HiCjfOAk8YpudSLFcPdyXOkDtP4kFKHd+Rh7EBI9c0ENsKZZ38P2F+ptxwqv4g==;" +
//                    "EndpointSuffix=core.windows.net";

    public static final String RELEASE_CONTAINER = "quickstartcontainer";
    public static final String BACKUP_CONTAINER = "target";
    BlobServiceClient blobServiceClient;
    BlobContainerClient sourceContainerClient;
    BlobContainerClient desContainerClient;

    public CopyData(String containerSource, String containerDes) {
//        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(CONNECTION_STRING).buildClient();
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential("flinkstr01", "fzAnZsiM0+YSM4hm2eo0QZo5HiCjfOAk8YpudSLFcPdyXOkDtP4kFKHd+Rh7EBI9c0ENsKZZ38P2F+ptxwqv4g==");
        String endpoint = "https://flinkstr01.blob.core.windows.net";
        blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();


        this.sourceContainerClient = blobServiceClient.getBlobContainerClient(containerSource);
        this.desContainerClient = blobServiceClient.getBlobContainerClient(containerDes);
    }

    public List<String> copyAll(){
        List<String> result = new ArrayList<>();
        for (BlobItem blobItem : sourceContainerClient.listBlobs()) {
            String blobName = blobItem.getName();
            BlobClient destBlobClient = desContainerClient.getBlobClient(blobName);
            BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(blobName);
        // generate sas token
        OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
        BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);

        BlobServiceSasSignatureValues myValues = new BlobServiceSasSignatureValues(expiryTime, permission)
                .setStartTime(OffsetDateTime.now());
        String sasToken =sourceBlobClient.generateSas(myValues);
            String res =destBlobClient.copyFromUrl(sourceBlobClient.getBlobUrl()  + "?" + sasToken);
            result.add(blobName);
        }
        return result;
    }

    public String copyBlob(String blobName){
        return copyBlob(blobName,blobName);

    }

    public String copyBlob(String blobNameSource, String blobNameDes){
        BlobClient destBlobClient = desContainerClient.getBlobClient(blobNameSource);
        BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(blobNameDes);
        String res =destBlobClient.copyFromUrl(sourceBlobClient.getBlobUrl());
        return res;
    }

    public static void main(String[] args) {
        CopyData copyData = new CopyData(RELEASE_CONTAINER, BACKUP_CONTAINER);
        System.out.println(copyData.copyAll());
    }

}

