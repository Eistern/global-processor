package org.eistern.processor;

public class TerminatingBatchAction<T, R> implements ProcessorAction<T, R> {
    private final BatchedExecutingQueue<T, R> executingQueue;

    TerminatingBatchAction(BatchedExecutingQueue<T, R> executingQueue) {
        this.executingQueue = executingQueue;
        this.executingQueue.start();
    }

    @Override
    public R apply(T t) {
        return executingQueue.offer(t).join();
    }
}
