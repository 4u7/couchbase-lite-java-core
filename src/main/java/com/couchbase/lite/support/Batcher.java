package com.couchbase.lite.support;

import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Utility that queues up objects until the queue fills up or a time interval elapses,
 * then passes objects, in groups of its capacity, to a client-supplied processor block.
 */
public class Batcher<T> {

    ///////////////////////////////////////////////////////////////////////////
    // Instance Variables
    ///////////////////////////////////////////////////////////////////////////

    private ScheduledExecutorService workExecutor;
    private int capacity = 0;
    private int delay = 0;
    private List<T> inbox;
    private boolean scheduled = false;
    private int scheduledDelay = 0;
    private BatchProcessor<T> processor;
    private long lastProcessedTime = 0;
    private ScheduledFuture pendingFuture = null;

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Initializes a batcher.
     *
     * @param workExecutor the work executor that performs actual work
     * @param capacity     The maximum number of objects to batch up. If the queue reaches
     *                     this size, the queued objects will be sent to the processor
     *                     immediately.
     * @param delay        The maximum waiting time in milliseconds to collect objects
     *                     before processing them. In some circumstances objects will be
     *                     processed sooner.
     * @param processor    The callback/block that will be called to process the objects.
     */
    public Batcher(ScheduledExecutorService workExecutor,
                   int capacity,
                   int delay,
                   BatchProcessor<T> processor) {
        this.workExecutor = workExecutor;
        this.capacity = capacity;
        this.delay = delay;
        this.processor = processor;
        this.inbox = new ArrayList<T>();
        this.scheduled = false;
        this.lastProcessedTime = System.currentTimeMillis();
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Methods - Public
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get capacity amount.
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Get delay amount.
     */
    public int getDelay() {
        return delay;
    }

    /**
     * The number of objects currently in the queue.
     */
    public synchronized int count() {
        return inbox.size();
    }

    /**
     * Adds an object to the queue.
     */
    public synchronized void queueObject(T object) {
        List<T> objects = Arrays.asList(object);
        queueObjects(objects);
    }

    /**
     * Adds multiple objects to the queue.
     */
    public synchronized void queueObjects(List<T> objects) {
        if (objects == null || objects.size() == 0)
            return;
        Log.v(Log.TAG_BATCHER, "%s: queueObjects called with %d objects (current inbox size = %d)",
                this, objects.size(), inbox.size());
        inbox.addAll(objects);
        notify();
        scheduleBatchProcess(false);
    }

    /**
     * Sends _all_ the queued objects at once to the processor block.
     * After this method returns, all **current** inbox items will be processed.
     * Note that this method schedules all items in the inbox and blocks until all items
     * are processed.
     */
    public void flushAll() {
        synchronized (this) {
            unschedule();
        }

        while (true) {
            ScheduledFuture future;
            synchronized (this) {
                if (inbox.size() == 0)
                    break; // Nothing to do

                final List<T> toProcess = new ArrayList<T>(inbox);
                inbox.clear();
                notify();

                future = workExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        processor.process(toProcess);
                        synchronized (Batcher.this) {
                            lastProcessedTime = System.currentTimeMillis();
                        }
                    }
                }, 0, TimeUnit.MILLISECONDS);
            }

            if (future != null && !future.isDone() && !future.isCancelled()) {
                try {
                    future.get();
                } catch (Exception e) {
                    Log.e(Log.TAG_BATCHER, "%s: Error while waiting for pending future " +
                            "when flushing all items", e, this);
                }
            }
        }
    }

    /**
     * Empties the queue without processing any of the objects in it.
     */
    public synchronized void clear() {
        unschedule();
        inbox.clear();
        notify();
    }

    /**
     * Wait for the **current** items in the queue to be all processed.
     */
    public void waitForPendingFutures() {
        // Wait inbox to become empty:
        Log.v(Log.TAG_BATCHER, "%s: waitForPendingFutures is called ...", this);

        while (true) {
            ScheduledFuture future;
            synchronized (this) {
                while (!inbox.isEmpty()) {
                    try {
                        Log.v(Log.TAG_BATCHER, "%s: waitForPendingFutures, inbox size: %d",
                                this, inbox.size());
                        wait();
                    } catch (InterruptedException e) {}
                }
                future = pendingFuture;
            }

            // Wait till ongoing computation completes:
            if (future != null && !future.isDone() && !future.isCancelled()) {
                try {
                    future.get();
                } catch (Exception e) {
                    Log.e(Log.TAG_BATCHER, "%s: Error while waiting for pending futures", e, this);
                }
            }

            synchronized (this) {
                if (inbox.isEmpty())
                    break;
            }
        }

        Log.v(Log.TAG_BATCHER, "%s: waitForPendingFutures done", this);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Methods - protected or private
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Schedule batch process based on capacity, inbox size, and last processed time.
     * @param immediate flag to schedule the batch process immediately regardless.
     */
    private synchronized void scheduleBatchProcess(boolean immediate) {
        if (inbox.size() == 0)
            return;

        int suggestedDelay = 0;
        if (!immediate && inbox.size() < capacity) {
            // Schedule the processing. To improve latency, if we haven't processed anything
            // in at least our delay time, rush these object(s) through ASAP:
            if (System.currentTimeMillis() - lastProcessedTime < delay)
                suggestedDelay = delay;
        }
        scheduleWithDelay(suggestedDelay);
    }

    /**
     * Schedule the batch processing with the delay. If there is one batch currently
     * in processing, the schedule will be ignored as after the processing is done,
     * the next batch will be rescheduled.
     * @param delay delay to schedule the work executor to process the next batch.
     */
    private synchronized void scheduleWithDelay(int delay) {
        if (scheduled && delay < scheduledDelay) {
            if (isPendingFutureReadyOrInProcessing()) {
                // Ignore as there is one batch currently in processing or ready to be processed:
                Log.v(Log.TAG_BATCHER, "%s: scheduleWithDelay: %d ms, ignored as current batch " +
                        "is ready or in process", this, delay);
                return;
            }
            unschedule();
        }

        if (!scheduled) {
            scheduled = true;
            scheduledDelay = delay;
            Log.v(Log.TAG_BATCHER, "%s: scheduleWithDelay %d ms, scheduled ...", this, delay);
            pendingFuture = workExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    processNow();
                }
            }, scheduledDelay, TimeUnit.MILLISECONDS);
        } else
            Log.v(Log.TAG_BATCHER, "%s: scheduleWithDelay %d ms, ignored", this, delay);
    }

    /**
     * Unschedule the scheduled batch processing.
     */
    private synchronized void unschedule() {
        if (pendingFuture != null && !pendingFuture.isDone() && !pendingFuture.isCancelled()) {
            Log.v(Log.TAG_BATCHER, "%s: cancelling the pending future ...", this);
            pendingFuture.cancel(false);
        }
        scheduled = false;
    }

    /**
     * Check if the current pending future is ready to be processed or in processing.
     * @return true if the current pending future is ready to be processed or in processing.
     * Otherwise false. Will also return false if the current pending future is done or cancelled.
     */
    private synchronized boolean isPendingFutureReadyOrInProcessing() {
        if (pendingFuture != null && !pendingFuture.isDone() && !pendingFuture.isCancelled()) {
            return pendingFuture.getDelay(TimeUnit.MILLISECONDS) <= 0;
        }
        return false;
    }

    /**
     * This method is called by the work executor to do the batch process.
     * The inbox items up to the batcher capacity will be taken out to process.
     * The next batch will be rescheduled if there are still some items left in the
     * inbox.
     */
    private void processNow() {
        List<T> toProcess;
        boolean scheduleNextBatchImmediately = false;
        synchronized (this) {
            int count = inbox.size();
            Log.v(Log.TAG_BATCHER, "%s: processNow() called, inbox size: %d", this, count);
            if (count == 0)
                return;
            else if (count <= capacity) {
                toProcess = new ArrayList<T>(inbox);
                inbox.clear();
            } else {
                toProcess = new ArrayList<T>(inbox.subList(0, capacity));
                for (int i = 0; i < capacity; i++)
                    inbox.remove(0);
                scheduleNextBatchImmediately = true;
            }
            notify();
        }

        if (toProcess != null && toProcess.size() > 0) {
            Log.v(Log.TAG_BATCHER, "%s: invoking processor %s with %d items",
                    this, processor, toProcess.size());
            processor.process(toProcess);
        } else {
            Log.v(Log.TAG_BATCHER, "%s: nothing to process", this);
        }

        synchronized (this) {
            lastProcessedTime = System.currentTimeMillis();
            scheduled = false;
            scheduleBatchProcess(scheduleNextBatchImmediately);
            Log.v(Log.TAG_BATCHER, "%s: invoking processor done",
                    this, processor, toProcess.size());
        }
    }
}
