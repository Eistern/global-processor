package org.eistern.processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

public class BatchedExecutingQueue<T, R> implements AutoCloseable {
    private final BlockingQueue<InputPair> inputElements;
    private final Function<List<T>, List<R>> batchedAction;

    private final ScheduledExecutorService batchTaskExecutor;

    private final int batchSize;
    private final Duration batchTimeout;

    public BatchedExecutingQueue(int batchSize, Duration batchTimeout, Function<List<T>, List<R>> batchedAction) {
        this.inputElements = new ArrayBlockingQueue<>(batchSize * 2);
        this.batchedAction = batchedAction;

        this.batchTaskExecutor = new ScheduledThreadPoolExecutor(1);

        this.batchSize = batchSize;
        this.batchTimeout = batchTimeout;
    }

    public CompletableFuture<R> offer(T value) {
        CompletableFuture<R> resultingFuture = new CompletableFuture<>();
        inputElements.offer(new InputPair(value, resultingFuture));

        if (inputElements.size() > batchSize) {
            batchTaskExecutor.execute(new BatchProcessingTask());
        }

        return resultingFuture;
    }

    public void start() {
        // init batch executor
        batchTaskExecutor.scheduleAtFixedRate(new BatchProcessingTask(), 0, batchTimeout.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        // close batch executor
    }

    private class BatchProcessingTask implements Runnable {

        @Override
        public void run() {
            List<InputPair> processedBatch = new ArrayList<>();
            int processedElements = inputElements.drainTo(processedBatch, batchSize);

            System.out.printf("Batch processing of %d elements%n", processedElements);

            List<T> processedValues = processedBatch.stream().map(p -> p.value).toList();
            List<R> results = batchedAction.apply(processedValues);

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
