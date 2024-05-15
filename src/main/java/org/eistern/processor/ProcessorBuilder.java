package org.eistern.processor;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.WillNotClose;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public class ProcessorBuilder<T, R> {
    private final @Nonnull ExecutorService processorExecutor;
    private final @Nonnull Function<T, R> processorAction;

    @Nonnull
    static <T1> ProcessorBuilder<T1, T1> create() {
        return create(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Nonnull
    static <T1> ProcessorBuilder<T1, T1> create(@WillNotClose ExecutorService customExecutor) {
        return new ProcessorBuilder<>(customExecutor, Function.identity());
    }

    private ProcessorBuilder(
            @WillNotClose ExecutorService processorExecutor,
            Function<T, R> processorAction
    ) {
        this.processorExecutor = processorExecutor;
        this.processorAction = processorAction;
    }

    public @Nonnull <S> ProcessorBuilder<T, S> map(Function<R, S> mapper) {
        Function<T, S> combined = processorAction.andThen(mapper);
        return new ProcessorBuilder<>(processorExecutor, combined);
    }

    public @Nonnull BatchedProcessorBuilder<R> batched(int batchMaxSize, Duration batchTimeout) {
        return new BatchedProcessorBuilder<>(processorExecutor, new ArrayList<>(), batchMaxSize, batchTimeout);
    }

    public @Nonnull ExecutorService processorExecutor() {
        return processorExecutor;
    }

    public @Nonnull Function<T, R> processorAction() {
        return processorAction;
    }

    public @Nonnull ObjectProcessor<T, R> build() {
        return new ObjectProcessor<>(this);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public class BatchedProcessorBuilder<R1> {
        private final @Nonnull ExecutorService processorExecutor;
        private final @Nonnull List<Function<List<?>, List<?>>> actionQueue;
        private final int batchSize;
        private final Duration batchTimeout;

        private BatchedProcessorBuilder(
                ExecutorService processorExecutor,
                List<Function<List<?>, List<?>>> actionQueue,
                int batchSize,
                Duration batchTimeout
        ) {
            this.processorExecutor = processorExecutor;
            this.actionQueue = actionQueue;
            this.batchSize = batchSize;
            this.batchTimeout = batchTimeout;
        }

        public @Nonnull <S1> BatchedProcessorBuilder<S1> map(Function<R1, S1> mapper) {
            actionQueue.add((Function) new ParallelBatchAction<>(mapper, processorExecutor));
            return new BatchedProcessorBuilder<>(processorExecutor, actionQueue, batchSize, batchTimeout);
        }

        public @Nonnull <S1> BatchedProcessorBuilder<S1> flatMap(Function<List<R1>, List<S1>> mapper) {
            actionQueue.add((Function) mapper);
            return new BatchedProcessorBuilder<>(processorExecutor, actionQueue, batchSize, batchTimeout);
        }

        public @Nonnull ProcessorBuilder<T, R1> sequential() {
            List<Function> collapsedQueue = collapseFunctionList();

            Function resultingListFunction = Function.identity();
            for (Function batchedFunction : collapsedQueue) {
                resultingListFunction = resultingListFunction.andThen(batchedFunction);
            }

            var completeBatchAction = (TerminatingBatchAction<R, R1>) new TerminatingBatchAction<>(
                    new BatchedExecutingQueue<Object, Object>(batchSize, batchTimeout, resultingListFunction)
            );

            Function<T, R1> combinedProcessorFunction = ProcessorBuilder.this.processorAction.andThen(completeBatchAction);
            return new ProcessorBuilder<>(processorExecutor, combinedProcessorFunction);
        }

        private List<Function> collapseFunctionList() {
            List<Function> collapsedList = new ArrayList<>();

            if (actionQueue.isEmpty()) {
                throw new IllegalStateException("batched() block must contain at least one mapping function");
            }

            Function previousFunction = null;
            for (Function currentFunction : actionQueue) {

                if (previousFunction == null) {
                    previousFunction = currentFunction;
                    continue;
                }

                if (previousFunction instanceof ParallelBatchAction previousBatchAction
                        && currentFunction instanceof ParallelBatchAction currentBatchAction) {
                    previousFunction = previousBatchAction.andThenSingular(currentBatchAction.baseFunction());
                    continue;
                }

                collapsedList.add(previousFunction);
                previousFunction = currentFunction;
            }
            collapsedList.add(previousFunction);
            return collapsedList;
        }
    }
}
