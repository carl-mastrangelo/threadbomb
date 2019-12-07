package com.carlmastrangelo.threadbomb;

import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.SplittableRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
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
  private final Histogram executeToRunLatency;
  private final Histogram scheduleToExecuteLatency;
  private final Executor exec;
  private final LongSupplier workPacer;
  private final LongSupplier workSupplier;

  // State
  private final LongAdder itemsComplete = new LongAdder();

  public static void main(String [] arg) {
    Histogram scheduleToExecuteLatency = new AtomicHistogram(Long.MAX_VALUE, 5);
    Histogram executeLatency = new AtomicHistogram(Long.MAX_VALUE, 5);
    Histogram executeToRunLatency = new AtomicHistogram(Long.MAX_VALUE, 5);

    long seed = new SplittableRandom().nextLong();
    seed = 5337644705666772232L;
    logger.info("Using seed of " + seed);
    SplittableRandom random = new SplittableRandom(seed);

    LongSupplier workPacer = () -> nextDelay(random, 500000);
    LongSupplier workSupplier = () -> 10_000_0L; // ns
    long testDurationNanos = TimeUnit.SECONDS.toNanos(60);

    /*
    BlockingQueue<Runnable> q = new LinkedBlockingDeque<>();
    ThreadPoolExecutor exec = new ThreadPoolExecutor(
        50, 50, 1, TimeUnit.MINUTES, q);
    exec.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    exec.setRejectedExecutionHandler(new BlockOnEnqueue(q));
     */
    ExecutorService exec = new ForkJoinPool(256);
    try {
      new Drop(
          exec, scheduleToExecuteLatency, executeLatency, executeToRunLatency, testDurationNanos, workPacer, workSupplier)
          .run();
    } finally {
      exec.shutdownNow();
    }
    log(System.err, scheduleToExecuteLatency, "ScheduleToExecute Latency");
    log(System.err, executeLatency, "Execute Latency");
    log(System.err, executeToRunLatency, "ExecuteToRun Latency");
  }

  Drop(
      Executor exec,
      Histogram scheduleToExecuteLatency,
      Histogram executeLatency,
      Histogram executeToRunLatency,
      long durationNanos,
      LongSupplier workPacer,
      LongSupplier workSupplier) {
    this.scheduleToExecuteLatency = scheduleToExecuteLatency;
    this.executeLatency = executeLatency;
    this.executeToRunLatency = executeToRunLatency;
    this.exec = exec;
    this.durationNanos = durationNanos;
    this.workPacer = workPacer;
    this.workSupplier = workSupplier;
  }

  void run() {
    long itemsSubmitted = 0;

    final long startTimeNanos = System.nanoTime();
    long nextWorkTime = startTimeNanos;
    long itemSubmittedNanos;

    do {
      WorkItem wi = new WorkItem(workSupplier.getAsLong(), nextWorkTime);
      long itemCreateNanos = System.nanoTime();
      exec.execute(wi);
      itemSubmittedNanos = System.nanoTime();
      itemsSubmitted++;
      long executeDurationNanos = itemSubmittedNanos - itemCreateNanos;
      executeLatency.recordValue(executeDurationNanos);
      scheduleToExecuteLatency.recordValue(itemCreateNanos - nextWorkTime);
      nextWorkTime += workPacer.getAsLong();
      // Attempt to keep pace even if execute took a long time.
      sleep(nextWorkTime - itemSubmittedNanos);
    } while (itemSubmittedNanos - startTimeNanos < durationNanos);

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
      // runLatency.recordValue(itemStop - itemStart);
      itemsComplete.increment();
    }
  }

  static void log(PrintStream out, Histogram histogram, String name) {
    out.println(name);

    out.println(MessageFormat.format("Total:  {0}", histogram.getTotalCount()));
    out.println(MessageFormat.format("Avg:    {0}ns", (long) histogram.getMean()));
    out.println(MessageFormat.format("Median: {0}ns", histogram.getValueAtPercentile(50)));
    out.println(MessageFormat.format("90%:    {0}ns", histogram.getValueAtPercentile(90)));
    out.println(MessageFormat.format("99%:    {0}ns", histogram.getValueAtPercentile(99)));
    out.println(MessageFormat.format("99.9%:  {0}ns", histogram.getValueAtPercentile(99.9)));
    out.println(MessageFormat.format("99.99%: {0}ns", histogram.getValueAtPercentile(99.99)));
    out.println(MessageFormat.format("100%:   {0}ns", histogram.getValueAtPercentile(100)));
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

  /**
   * Returns the exponentially distributed next delay time in nanoseconds.
   * @param random
   * @param itemsPerSecond
   * @return
   */
  private static long nextDelay(SplittableRandom random, double itemsPerSecond) {
    double seconds = -Math.log(Math.max(random.nextDouble(), DELAY_EPSILON)) / itemsPerSecond;
    double nanos = seconds * 1_000_000_000.;
    return Math.round(nanos);
  }

  private static final class BlockOnEnqueue implements RejectedExecutionHandler {

    private final BlockingQueue<Runnable> queue;

    BlockOnEnqueue(BlockingQueue<Runnable> queue) {
      this.queue = queue;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      try {
        queue.put(r);
      } catch (InterruptedException e) {
        throw new RejectedExecutionException(e);
      }
    }
  }
}
