package org.eistern.processor;

import java.util.function.Function;

public interface ProcessorAction<T, R> extends Function<T, R> {
}
