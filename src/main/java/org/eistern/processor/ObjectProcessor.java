package org.eistern.processor;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.WillNotClose;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public class ObjectProcessor<T, R> {
    public static <T> ProcessorBuilder<T, T> builder() {
        return ProcessorBuilder.create();
    }

    public static <T> ProcessorBuilder<T, T> builder(@WillNotClose ExecutorService customExecutor) {
        return ProcessorBuilder.create(customExecutor);
    }

    private final @Nonnull ExecutorService processorExecutor;
    private final @Nonnull Function<T, R> processorAction;

    ObjectProcessor(ProcessorBuilder<T, R> builder) {
        this.processorExecutor = builder.processorExecutor();
        this.processorAction = builder.processorAction();
    }

    public CompletableFuture<R> apply(T value) {
        return CompletableFuture.supplyAsync(() -> processorAction.apply(value), processorExecutor);
    }
}
