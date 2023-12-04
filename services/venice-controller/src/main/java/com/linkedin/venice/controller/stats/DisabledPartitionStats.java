package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class DisabledPartitionStats extends AbstractVeniceStats {
  private final Sensor disabledPartitionCount;

  public DisabledPartitionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    disabledPartitionCount = registerSensorIfAbsent("disabled_partition_count", new Count());
  }

  public void recordDisabledPartition() {
    disabledPartitionCount.record();
  }

}