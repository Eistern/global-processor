package org.eistern;

import org.eistern.processor.ObjectProcessor;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class Main {
    public static void main(String[] args) {
        System.out.print("Hello and welcome!");

        var objectProcessor = ObjectProcessor.<String>builder()
                .batched(2, Duration.ofSeconds(300))
                .map(String::length)
                .map(Integer::toBinaryString)
                .sequential()
                .build();

        CompletableFuture<String> f1 = objectProcessor.apply("1");
        CompletableFuture<String> f2 = objectProcessor.apply("11");
        CompletableFuture<String> f3 = objectProcessor.apply("111");
        CompletableFuture<String> f4 = objectProcessor.apply("1111");

        System.out.println(f1.join());
        System.out.println(f2.join());
        System.out.println(f3.join());
        System.out.println(f4.join());
    }
}