package org.eistern.processor;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

public class ParallelBatchAction<T, R> implements ProcessorAction<List<T>, List<R>> {
    private final Function<T, R> baseFunction;
    private final ExecutorService workerExecutor;

    ParallelBatchAction(Function<T, R> baseFunction, ExecutorService workerExecutor) {
        this.baseFunction = baseFunction;
        this.workerExecutor = workerExecutor;
    }

    public <V> ParallelBatchAction<T, V> andThenSingular(Function<R, V> after) {
        Function<T, V> combinedFunction = baseFunction.andThen(after);
        return new ParallelBatchAction<>(combinedFunction, workerExecutor);
    }

    public Function<T, R> baseFunction() {
        return baseFunction;
    }

    @Override
    public List<R> apply(List<T> value) {
        List<CompletableFuture<R>> futureList = value.stream()
                .map(v -> (Supplier<R>) (() -> baseFunction.apply(v)))
                .map(v -> CompletableFuture.supplyAsync(v, workerExecutor))
                .toList();

        return futureList.stream()
                .map(CompletableFuture::join)
                .toList();
    }
}
