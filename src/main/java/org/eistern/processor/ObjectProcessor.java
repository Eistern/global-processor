package org.eistern.processor;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.WillNotClose;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public class ObjectProcessor<T, R> {
    private static final ExecutorService DEFAULT_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    public static <T> ProcessorBuilder<T, T> builder() {
        return ProcessorBuilder.create(DEFAULT_EXECUTOR);
    }

    public static <T> ProcessorBuilder<T, T> builder(@WillNotClose ExecutorService customExecutor) {
        return ProcessorBuilder.create(customExecutor);
    }

    private final @Nonnull Function<T, R> processorAction;

    ObjectProcessor(ProcessorBuilder<T, R> builder) {
        this.processorAction = builder.processorAction();
    }

    public R apply(T value) {
        return processorAction.apply(value);
    }
}
