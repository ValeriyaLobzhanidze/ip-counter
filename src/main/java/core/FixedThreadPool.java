package core;

import java.util.concurrent.*;

/**
 * Class that helps to control exact number of threads in the thread pool;
 *
 * Though we could just use ThreadPoolExecutor with CallerRunPolicy, this approach has its drawbacks and limitations;
 * In particular whenever BlockingQueue is full ThreadPoolExecutor with such a policy would just execute the task in the caller thread,
 * which might be undesirable and somewhat counterintuitive;
 * */
public class FixedThreadPool {
    private final Semaphore semaphore;
    private final ExecutorService executor;
    private Exception firstInternalException;

    public FixedThreadPool(int threadAmount) {
        this.semaphore = new Semaphore(threadAmount);
        this.executor = Executors.newFixedThreadPool(threadAmount);
    }

    public void execute(Runnable runnable) {
        try {
            semaphore.acquire();
            executor.execute(() -> {
                try {
                    runnable.run();
                } catch (Exception e) {
                    if (firstInternalException == null) {
                        firstInternalException = e;
                        executor.shutdownNow();
                    }
                } finally {
                    semaphore.release();
                }
            });

        } catch (RejectedExecutionException ex) {
            if (firstInternalException != null) {
                throw new RuntimeException(firstInternalException);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) {
        try {
            return executor.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
