package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.davinci.ingestion.channel.IngestionServiceChannelInitializer;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.IngestionUtils.*;
import static java.lang.Thread.*;


/**
 * IngestionService is the server service of the ingestion isolation side.
 */
public class IngestionService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(IngestionService.class);
  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ExecutorService ingestionExecutor = Executors.newFixedThreadPool(10);

  private ChannelFuture serverFuture;
  private MetricsRepository metricsRepository = null;
  private VeniceConfigLoader configLoader = null;
  private SubscriptionBasedReadOnlyStoreRepository storeRepository = null;
  private StorageService storageService = null;
  private KafkaStoreIngestionService storeIngestionService = null;
  private StorageMetadataService storageMetadataService = null;
  // PartitionState and StoreVersionState serializers are lazily constructed after receiving the init configs
  private InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;
  private InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer;
  private boolean isInitiated = false;


  private final int servicePort;
  private IngestionRequestClient reportClient;

  //TODO: move netty config to a config file
  private static int nettyBacklogSize = 1000;

  public Map<String, Map<Integer, CompletableFuture<IngestionTaskReport>>> topicPartitionIngestionFuture = new VeniceConcurrentHashMap<>();

  public IngestionService(int servicePort) {
    this.servicePort = servicePort;

    // Initialize Netty server.
    Class<? extends ServerChannel> serverSocketChannelClass = NioServerSocketChannel.class;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(serverSocketChannelClass)
        .childHandler(new IngestionServiceChannelInitializer(this))
        .option(ChannelOption.SO_BACKLOG, nettyBacklogSize)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
    logger.info("IngestionService created");
  }

  @Override
  public boolean startInner() throws Exception {
    IngestionUtils.releaseTargetPortBinding(servicePort);
    int maxAttempt = 100;
    int retryCount = 0;
    while (true) {
      try {
        serverFuture = bootstrap.bind(servicePort).sync();
        break;
      } catch (Exception e) {
        retryCount += 1;
        if (retryCount > maxAttempt) {
          throw new VeniceException("Ingestion Service is unable to bind to target port " + servicePort  + " after " + maxAttempt + " retries.");
        }
      }
      Utils.sleep(100);
    }
    logger.info("Listener service started on port: " + servicePort);
    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();

    try {
      storeIngestionService.stop();
      storageService.stop();
    } catch (Throwable e) {
      throw new VeniceException("Unable to stop Ingestion Service", e);
    }

    ingestionExecutor.shutdown();
    try {
      if (!ingestionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        ingestionExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
  }

  public void setConfigLoader(VeniceConfigLoader configLoader) {
    this.configLoader = configLoader;
  }

  public void setStoreRepository(SubscriptionBasedReadOnlyStoreRepository storeRepository) {
    this.storeRepository = storeRepository;
  }

  public void setStorageService(StorageService storageService) {
    this.storageService = storageService;
  }

  public void setStoreIngestionService(KafkaStoreIngestionService storeIngestionService) {
    this.storeIngestionService = storeIngestionService;
  }

  public void setStorageMetadataService(StorageMetadataService storageMetadataService) {
    this.storageMetadataService = storageMetadataService;
  }

  public void setReportClient(IngestionRequestClient reportClient) {
    this.reportClient = reportClient;
  }

  public void setMetricsRepository(MetricsRepository metricsRepository) {
    this.metricsRepository = metricsRepository;
  }

  public void setPartitionStateSerializer(InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this.partitionStateSerializer = partitionStateSerializer;
  }

  public void setStoreVersionStateSerializer(
      InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer) {
    this.storeVersionStateSerializer = storeVersionStateSerializer;
  }

  public IngestionRequestClient getReportClient() {
    return reportClient;
  }

  public void setInitiated(boolean initiated) {
    isInitiated = initiated;
  }

  public boolean isInitiated() {
    return isInitiated;
  }

  public StorageService getStorageService() {
    return storageService;
  }

  public VeniceConfigLoader getConfigLoader() {
    return configLoader;
  }

  public KafkaStoreIngestionService getStoreIngestionService() {
    return storeIngestionService;
  }

  public StorageMetadataService getStorageMetadataService() {
    return storageMetadataService;
  }

  public SubscriptionBasedReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public InternalAvroSpecificSerializer<PartitionState> getPartitionStateSerializer() {
    return partitionStateSerializer;
  }

  public InternalAvroSpecificSerializer<StoreVersionState> getStoreVersionStateSerializer() {
    return storeVersionStateSerializer;
  }

  public ExecutorService getIngestionExecutor() {
    return ingestionExecutor;
  }

  public void reportIngestionCompletion(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    long offset = report.offset;
    logger.info("Ingestion completed for topic: " + topicName + ", partition id: " + partitionId + ", offset: " + offset);

    VeniceStoreConfig storeConfig = getConfigLoader().getStoreConfig(topicName);
    // Make sure partition is not consuming so we can safely close the rocksdb partition
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partitionId, 1, 60);
    getStorageService().getStorageEngineRepository().getLocalStorageEngine(topicName).closePartition(partitionId);
    logger.info("Closed partition: " + partitionId + " of topic: " + topicName);

    OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topicName, partitionId);
    logger.info("OffsetRecord of topic " + topicName + " , partition " + partitionId + " " + offsetRecord.toString());
    report.offsetRecord = ByteBuffer.wrap(offsetRecord.toBytes());
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topicName);
    if (storeVersionState.isPresent()) {
      report.storeVersionState = ByteBuffer.wrap(IngestionUtils.serializeStoreVersionState(topicName, storeVersionState.get()));
    } else {
      throw new VeniceException("StoreVersionState does not exist for version " + topicName);
    }

    byte[] serializedReport = serializeIngestionTaskReport(report);
    try {
      logger.info("Sending ingestion completion report for version:  " + topicName + " partition id:" + partitionId + " offset:" + offset);
      reportClient.sendRequest(reportClient.buildHttpRequest(IngestionAction.REPORT, serializedReport));
    } catch (Exception e) {
      logger.warn("Failed to send report with exception for topic: " + topicName + ", partition: " + partitionId , e);
    }
  }

  public void reportIngestionError(IngestionTaskReport report) {
    String topicName = report.topicName.toString();
    int partitionId = report.partitionId;
    logger.warn("Ingestion error for topic: " + topicName + ", partition id: " + partitionId + " " + report.errorMessage);

    VeniceStoreConfig storeConfig = getConfigLoader().getStoreConfig(topicName);
    // Make sure partition is not consuming so we can safely close the rocksdb partition
    getStoreIngestionService().stopConsumptionAndWait(storeConfig, partitionId, 10, 10);
    getStorageService().getStorageEngineRepository().getLocalStorageEngine(topicName).closePartition(partitionId);
    logger.info("Close error partition: " + partitionId + " of topic: " + topicName);

    byte[] serializedReport = serializeIngestionTaskReport(report);
    try {
      logger.info("Sending ingestion error report for version:  " + topicName + " partition id:" + partitionId);
      reportClient.sendRequest(reportClient.buildHttpRequest(IngestionAction.REPORT, serializedReport));
    } catch (Exception ex) {
      logger.warn("Failed to send report with exception for topic: " + topicName + ", partition: " + partitionId , ex);
    }
  }

  public static void main(String[] args) throws Exception {
    logger.info("Capture arguments: " + Arrays.toString(args));
    if (args.length != 1) {
      throw new VeniceException("Expected one arguments: port. Got " + args.length);
    }
    int port = Integer.parseInt(args[0]);
    IngestionService ingestionService = new IngestionService(port);
    ingestionService.startInner();
  }
}