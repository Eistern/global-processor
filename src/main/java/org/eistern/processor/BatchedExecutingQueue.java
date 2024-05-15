package org.eistern.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

public class BatchedExecutingQueue<T, R> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BatchedExecutingQueue.class);

    private final BlockingQueue<InputPair> inputElements;
    private final Function<List<T>, List<R>> batchedAction;

    private final ScheduledExecutorService batchTaskExecutor;

    private final int batchSize;
    private final Duration batchTimeout;

    public BatchedExecutingQueue(int batchSize, Duration batchTimeout, Function<List<T>, List<R>> batchedAction) {
        this.inputElements = new ArrayBlockingQueue<>(batchSize * 2);
        this.batchedAction = batchedAction;
        this.batchTaskExecutor = Executors.newSingleThreadScheduledExecutor();
        this.batchSize = batchSize;
        this.batchTimeout = batchTimeout;
    }

    public CompletableFuture<R> offer(T value) {
        CompletableFuture<R> resultingFuture = new CompletableFuture<>();
        inputElements.add(new InputPair(value, resultingFuture));

        if (inputElements.size() >= batchSize) {
            log.debug("Full batch of size {} gathered. Add new processing task into the executor", batchSize);
            batchTaskExecutor.execute(new BatchProcessingTask());
        }

        return resultingFuture;
    }

    public void start() {
        batchTaskExecutor.scheduleAtFixedRate(new BatchProcessingTask(), 0, batchTimeout.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        batchTaskExecutor.shutdown();
    }

    private class BatchProcessingTask implements Runnable {

        @Override
        public void run() {
            List<InputPair> processedBatch = new ArrayList<>();

            int processedElements = inputElements.drainTo(processedBatch, batchSize);
            log.debug("Start batch processing of {} elements", processedElements);

            List<T> processedValues = processedBatch.stream().map(p -> p.value).toList();
            List<R> results;
            try {
                results = batchedAction.apply(processedValues);
            } catch (Exception e) {
                log.error("Processing failed with exception. Failing {} futures", processedElements, e);
                for (int i = 0; i < processedElements; i++) {
                    processedBatch.get(i).future.completeExceptionally(e);
                }
                return;
            }
            log.debug("Processing successful. Complete {} futures", processedElements);
            for (int i = 0; i < processedElements; i++) {
                processedBatch.get(i).future.complete(results.get(i));
            }
        }
    }

    private class InputPair {
        private final T value;
        private final CompletableFuture<R> future;

        private InputPair(T value, CompletableFuture<R> future) {
            this.value = value;
            this.future = future;
        }
    }
}
