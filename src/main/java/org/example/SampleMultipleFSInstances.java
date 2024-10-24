package org.example;

import com.oracle.bmc.hdfs.BmcFilesystem;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;
import java.util.Random;

public class SampleMultipleFSInstances {


  private final String namespace;

  private final String bucket;
  private Configuration config;

  final String uri;
  final String fullPath ;

  BmcFilesystem fs;

  public static void main(final String[] args) throws Exception {
//    if (args.length != 2) {
//      throw new IllegalArgumentException(
//          "Must have 2 args: 1) path to config file, 2) object storage namespace");
//    }

//    for(int i=0;i<(40000000/5);i++){
//      veryLongString += "hello\n";
//    }
    System.out.println("Before new instances, current active thread count: " + Thread.activeCount());
    Thread.sleep(10000);

    // Use a thread-safe list
    List<SampleMultipleFSInstances> instList = new CopyOnWriteArrayList<>();

    // Add 100 instances of SampleMultileFSInstances using parallel stream
    IntStream.range(0, 4).forEach(i -> {
      final SampleMultipleFSInstances inst = new SampleMultipleFSInstances("/Users/teaugene/github/OciThreadLeak/src/main/resources/ociLocal.xml", "idic1l2s171r", "b" +(i+1));
      try {
        inst.createInstance();
        System.out.println("After created " + ((int)i+1) + " new instances, current active thread count: " + Thread.activeCount());
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      instList.add(inst);  // Add job to the thread-safe list
    });

    System.out.println("After created new instances, current active thread count: " + Thread.activeCount());
    Thread.sleep(10000);

    instList.stream().forEach((inst) -> {
      try {
        inst.close();
      } catch (Exception e) {
        System.out.println("exception in inst.close");
      }
    });
    IntStream.range(0, 100).forEach((i) -> {
      System.out.println(i * 10 + " seconds after close instances, current active thread count: " + Thread.activeCount());
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    System.exit(0);
  }

  public SampleMultipleFSInstances(String configurationFilePath, String namespace, String bucket) {
    this.config = createConfiguration(configurationFilePath);
    this.namespace = namespace;
    this.bucket = bucket;

    if (!new File(configurationFilePath).exists()) {
      throw new IllegalArgumentException(
          "File '" + configurationFilePath + "' does not exist");
    }
    final String authority = this.bucket + "@" + this.namespace;
    uri = "oci://" + authority;
    final String file = "object-" + RandomStringUtils.randomGraph(3, 10);
    fullPath = uri + "/" + file;
  }

  private Configuration createConfiguration(final String configFilePath) {
    final Configuration configuration = new Configuration();
    configuration.addResource(new Path(configFilePath));
    return configuration;
  }

  private static byte[] generateRandomBytes(int size) {
    Random random = new Random();
    byte[] randomBytes = new byte[size];
    random.nextBytes(randomBytes);
    return randomBytes;
  }
  public void createInstance() throws URISyntaxException, IOException {
    fs = new BmcFilesystem();
    fs.initialize(new URI(uri), config);
    fs.delete(new Path(fullPath), true);
    final FSDataOutputStream output = fs.create(new Path(fullPath));
    int sizeInMB = 40;
    int sizeInBytes = sizeInMB * 1024 * 1024;
    output.write( generateRandomBytes(sizeInBytes));
    output.close();
    IntStream.rangeClosed(0, 20).parallel().forEach((i) -> {
      try (FSDataInputStream inputStream = fs.open(new Path(fullPath))) {
        inputStream.read();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public void close() throws IOException {
    fs.delete(new Path(fullPath), true);
    fs.close();
  }
}