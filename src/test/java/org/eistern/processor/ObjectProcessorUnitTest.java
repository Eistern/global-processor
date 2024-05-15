package org.eistern.processor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("AssertBetweenInconvertibleTypes") //actual idea bug with assertj and completable futures
public class ObjectProcessorUnitTest {
    private static final String EXAMPLE_INPUT = "ascii-example";

    private final ExecutorService usedTestExecutor = Executors.newFixedThreadPool(2);

    @AfterEach
    void shutdownExecutor() {
        usedTestExecutor.shutdown();
    }

    @Test
    void testBasicChaining() {
        ObjectProcessor<String, Integer> testedProcessor = ObjectProcessor.<String>builder()
                .map(String::getBytes)
                .map(arr -> arr.length)
                .build();

        assertThat(testedProcessor)
                .returns(13, p -> p.apply(EXAMPLE_INPUT));
    }

    @Test
    void testBasicBatchedExecution() {
        ObjectProcessor<String, Integer> testedProcessor = ObjectProcessor.<String>builder(usedTestExecutor)
                .batched(2, Duration.ofSeconds(1))
                .map(String::length)
                .sequential()
                .map(i -> i + 1)
                .build();

        assertThat(CompletableFuture.supplyAsync(() -> testedProcessor.apply(EXAMPLE_INPUT)))
                .succeedsWithin(Duration.ofSeconds(2))
                .isEqualTo(14);
    }

    @Test
    void testChainingBatchedExecution() {
        ObjectProcessor<String, Integer> testedProcessor = ObjectProcessor.<String>builder(usedTestExecutor)
                .batched(2, Duration.ofSeconds(1))
                .map(String::length)
                .map(i -> i * 2)
                .sequential()
                .build();

        assertThat(CompletableFuture.supplyAsync(() -> testedProcessor.apply(EXAMPLE_INPUT)))
                .succeedsWithin(Duration.ofSeconds(2))
                .isEqualTo(26);
    }

    @Test
    void testFullBatchProcessing() {
        ObjectProcessor<String, Integer> testedProcessor = ObjectProcessor.<String>builder(usedTestExecutor)
                .batched(2, Duration.ofSeconds(300))
                .map(String::length)
                .sequential()
                .build();

        List<CompletableFuture<Integer>> futures = List.of(
                CompletableFuture.supplyAsync(() -> testedProcessor.apply(EXAMPLE_INPUT)),
                CompletableFuture.supplyAsync(() -> testedProcessor.apply(EXAMPLE_INPUT))
        );

        assertThat(futures)
                .allSatisfy(processingFuture -> assertThat(processingFuture)
                        .succeedsWithin(Duration.ofSeconds(150))
                        .isEqualTo(13)
                );
    }

    @Test
    void testFlatMapBatchProcessing() {
        ObjectProcessor<String, Integer> testedProcessor = ObjectProcessor.<String>builder(usedTestExecutor)
                .batched(2, Duration.ofSeconds(1))
                .flatMap(batch -> {
                    int batchSize = batch.size();
                    return batch.stream()
                            .map(v -> batchSize)
                            .toList();
                })
                .map(i -> i + 1)
                .sequential()
                .build();

        assertThat(testedProcessor)
                .returns(2, p -> p.apply(EXAMPLE_INPUT));
    }

    @Test
    void testPropagatesExceptionFromBatchedExecution() {
        ObjectProcessor<String, String> testedProcessor = ObjectProcessor.<String>builder(usedTestExecutor)
                .batched(2, Duration.ofSeconds(300))
                .flatMap(batch -> {
                    if (!batch.isEmpty()) {
                        throw new RuntimeException(EXAMPLE_INPUT);
                    }
                    return batch;
                })
                .sequential()
                .build();

        List<CompletableFuture<String>> futures = List.of(
                CompletableFuture.supplyAsync(() -> testedProcessor.apply(EXAMPLE_INPUT)),
                CompletableFuture.supplyAsync(() -> testedProcessor.apply(EXAMPLE_INPUT + "1"))
        );

        assertThat(futures)
                .allSatisfy(processingFuture -> assertThat(processingFuture)
                        .failsWithin(Duration.ofSeconds(150))
                        .withThrowableThat()
                        .withMessageContaining(EXAMPLE_INPUT)
                );
    }

    @Test
    @Disabled("Must be implemented with batched().map(), BatchProcessingTask refactoring")
    void testPropagatesCorrectExceptionDuringBasicMap() {
        ObjectProcessor<String, String> testedProcessor = ObjectProcessor.<String>builder(usedTestExecutor)
                .batched(2, Duration.ofSeconds(300))
                .map(el -> {
                    if (!el.equals(EXAMPLE_INPUT)) {
                        throw new RuntimeException("expected");
                    }
                    return el;
                })
                .sequential()
                .build();

        List<CompletableFuture<String>> futures = List.of(
                CompletableFuture.supplyAsync(() -> testedProcessor.apply(EXAMPLE_INPUT)),
                CompletableFuture.supplyAsync(() -> testedProcessor.apply(EXAMPLE_INPUT + "1"))
        );

        assertThat(futures)
                .anySatisfy(f -> assertThat(f)
                        .failsWithin(Duration.ofSeconds(150))
                        .withThrowableThat()
                        .withMessage("expected")
                )
                .anySatisfy(f -> assertThat(f)
                        .succeedsWithin(Duration.ofSeconds(150))
                        .isEqualTo(EXAMPLE_INPUT)
                );
    }
}
