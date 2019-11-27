package com.carlmastrangelo.threadbomb;

import java.io.PrintStream;
import java.util.SplittableRandom;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

public final class Drop {

  private static final Logger logger = Logger.getLogger(Drop.class.getName());

  // How long should the test run
  private final long durationNanos;
  private final Histogram executeLatency;
  private final Histogram executeToRunLatency = new AtomicHistogram(Long.MAX_VALUE, 4);
  private final Histogram runLatency = new AtomicHistogram(Long.MAX_VALUE, 4);
  private final Executor exec;
  private final LongSupplier workPacer;
  private final LongSupplier workSupplier;

  // State
  private final LongAdder itemsComplete = new LongAdder();

  public static void main(String [] arg) {
    Histogram executeLatency = new AtomicHistogram(Long.MAX_VALUE, 5);

    SplittableRandom random = new SplittableRandom();
    LongSupplier workPacer = () -> nextDelay(random, 10000);
    LongSupplier workSupplier = () -> 10_000L; // 1ms
    long testDurationNanos = TimeUnit.SECONDS.toNanos(10);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      new Drop(exec, executeLatency, testDurationNanos, workPacer, workSupplier).run();
    } finally {
      exec.shutdownNow();
    }
    log(System.err, executeLatency, "Execute Latency");
  }

  Drop(
      Executor exec,
      Histogram executeLatency,
      long durationNanos,
      LongSupplier workPacer,
      LongSupplier workSupplier) {
    this.executeLatency = executeLatency;
    this.exec = exec;
    this.durationNanos = durationNanos;
    this.workPacer = workPacer;
    this.workSupplier = workSupplier;
  }

  void run() {
    long itemsSubmitted = 0;

    final long startTimeNanos = System.nanoTime();
    long nextWorkTime = startTimeNanos;
    long itemCreateNanos = startTimeNanos;

    while (itemCreateNanos - startTimeNanos < durationNanos) {
      WorkItem wi = new WorkItem(workSupplier.getAsLong(), itemCreateNanos);
      exec.execute(wi);
      long itemSubmittedNanos = System.nanoTime();
      itemsSubmitted++;
      long executeDurationNanos = itemSubmittedNanos - itemCreateNanos;
      executeLatency.recordValue(executeDurationNanos);
      nextWorkTime += workPacer.getAsLong();
      // Attempt to keep pace even if execute took a long time.
      sleep(nextWorkTime - itemSubmittedNanos);
      itemCreateNanos = System.nanoTime();
    }

    logger.info("Run complete, waiting for completion");
    long itemsCompletedSnapshot;
    long lastPrintTime = System.nanoTime();
    while ((itemsCompletedSnapshot = itemsComplete.longValue()) != itemsSubmitted) {
      if (System.nanoTime() - lastPrintTime > TimeUnit.SECONDS.toNanos(1)) {
        logger.log(Level.INFO, "Waiting on {0} items", new Object[]{itemsSubmitted - itemsCompletedSnapshot});
        lastPrintTime = System.nanoTime();
      }
      Thread.yield();
    }
  }

  private final class WorkItem implements Runnable {
    final long durationNanos;
    final long creationNanos;

    WorkItem(long durationNanos, long creationNanos) {
      this.creationNanos = creationNanos;
      this.durationNanos = durationNanos;
    }

    @Override
    public void run() {
      long itemStart = System.nanoTime();
      sleep(durationNanos);
      long itemStop = System.nanoTime();
      executeToRunLatency.recordValue(itemStart - creationNanos);
      runLatency.recordValue(itemStop - itemStart);
      itemsComplete.increment();
    }
  }

  static void log(PrintStream out, Histogram histogram, String name) {
    out.println(name);
    out.println("Total:  " + histogram.getTotalCount());
    out.println("Avg:    " + histogram.getMean());
    out.println("Median: " + histogram.getValueAtPercentile(50.0));
    out.println("90%:    " + histogram.getValueAtPercentile(90.0));
    out.println("99%:    " + histogram.getValueAtPercentile(99.0));
    out.println("99.9%:  " + histogram.getValueAtPercentile(99.9));
    out.println("99.99%: " + histogram.getValueAtPercentile(99.99));
    out.println("100%:   " + histogram.getValueAtPercentile(100.0));
  }

  private static void sleep(long ns) {
    if (ns <= 0) {
      return;
    }
    long start = System.nanoTime();
    long remaining;
    while ((remaining = (ns - System.nanoTime() + start)) > 0) {
      if (remaining > 100_000L) {
        LockSupport.parkNanos(remaining >> 3);
      }
      Thread.onSpinWait();
    }
  }

  private static final double DELAY_EPSILON = Math.nextUp(0d);

  private static long nextDelay(SplittableRandom random, double itemsPerSecond) {
    double seconds = -Math.log(Math.max(random.nextDouble(), DELAY_EPSILON)) / itemsPerSecond;
    double nanos = seconds * 1_000_000_000.;
    return Math.round(nanos);
  }
}
