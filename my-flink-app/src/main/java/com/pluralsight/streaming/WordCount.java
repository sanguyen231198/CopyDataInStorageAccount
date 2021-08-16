package com.pluralsight.streaming;

//import com.azure.storage.blob.BlobClient;
//import com.azure.storage.blob.BlobContainerClient;
//import com.azure.storage.blob.BlobServiceClient;
//import com.azure.storage.blob.BlobServiceClientBuilder;
//import com.azure.storage.blob.models.BlobItem;
//import com.azure.storage.common.StorageSharedKeyCredential;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.FileSystemBlobStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class WordCount {
//    public static final String RELEASE_CONTAINER = "quickstartcontainer";
//    public static final String BACKUP_CONTAINER = "target";
////    BlobServiceClient blobServiceClient;
////    BlobContainerClient sourceContainerClient;
////    BlobContainerClient desContainerClient;
//    BlobStoreService blobStoreService;
//
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//
//        StorageSharedKeyCredential credential = new StorageSharedKeyCredential("flinkstr01", "fzAnZsiM0+YSM4hm2eo0QZo5HiCjfOAk8YpudSLFcPdyXOkDtP4kFKHd+Rh7EBI9c0ENsKZZ38P2F+ptxwqv4g==");
//        String endpoint = "https://flinkstr01.blob.core.windows.net";
//        BlobStoreService blobStoreService = new FileSystemBlobStore()
//        BlobServiceClient  blobServiceClient = new BlobServiceClientBuilder()
//                .endpoint(endpoint)
//                .credential(credential)
//                .buildClient();
//        BlobContainerClient sourceContainerClient = blobServiceClient.getBlobContainerClient(RELEASE_CONTAINER);
//        for (BlobItem blobItem : sourceContainerClient.listBlobs()) {
//            String blobName = blobItem.getName();
////            BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(blobName);
//            BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(blobName);
////            DataStream<String> text = env.readTextFile("wasbs://flink@nifiteststr01.blob.core.windows.net/input/");
//            DataStream<String> text = env.readFile(sourceBlobClient);
//        }
//
//
//
//
////
////        DataStream<Tuple2<String, Integer>> dataStream = text
////                .flatMap(new Splitter())
////                .keyBy(value -> value.f0)
////                .sum(1);
//
//
////          dataStream.print();
////        dataStream.writeAsText("wasbs://target@nifiteststr01.blob.core.windows.net/output/output.txt");
////        dataStream.addSink
//
//        env.execute("Text WordCount");
//    }
//
//    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
//        @Override
//        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
//            for (String word: sentence.split(" ")) {
//                out.collect(new Tuple2<>(word, 1));
//            }
//        }
//    }
}
