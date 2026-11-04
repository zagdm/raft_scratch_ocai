package com.myraft.raft.timer;

import com.myraft.config.RaftProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ElectionTimer {
    private static final Logger log = LoggerFactory.getLogger(ElectionTimer.class);
    private final RaftProperties raftProperties;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "raft-election-timer");
        t.setDaemon(true);
        return t;
    });
    private final Object lock = new Object();
    private final AtomicLong epoch = new AtomicLong(0);
    private volatile long currentEpoch = -1;
    private ScheduledFuture<?> scheduledFuture;
    private volatile Runnable timeoutCallback;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ElectionTimer(RaftProperties raftProperties) {
        this.raftProperties = raftProperties;
    }

    public void setTimeoutCallback(Runnable timeoutCallback) {
        synchronized (lock) {
            this.timeoutCallback = timeoutCallback;
        }
    }

    public void start() {
        synchronized (lock) {
            running.set(true);
            resetInternalLocked();
        }
    }

    public void reset() {
        synchronized (lock) {
            resetInternalLocked();
        }
    }

    private void resetInternalLocked() {
        Runnable cb = timeoutCallback;
        if (cb == null) {
            return;
        }
        long nextEpoch = epoch.incrementAndGet();
        currentEpoch = nextEpoch;

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        int min = raftProperties.getElectionTimeoutMinMs();
        int max = raftProperties.getElectionTimeoutMaxMs();
        int timeout = min + (int) (Math.random() * Math.max(1, max - min + 1));
        log.trace("election timer reset timeoutMs={} epoch={}", timeout, nextEpoch);

        final long scheduledEpoch = nextEpoch;
        scheduledFuture = scheduler.schedule(() -> onTimeout(scheduledEpoch), timeout, TimeUnit.MILLISECONDS);
    }

    private void onTimeout(long scheduledEpoch) {
        Runnable cb;
        synchronized (lock) {
            if (scheduledEpoch != currentEpoch) {
                log.trace("raft.election.timeout.ignored epoch={} currentEpoch={}", scheduledEpoch, currentEpoch);
                return;
            }
            cb = timeoutCallback;
        }
        if (cb == null) {
            return;
        }
        log.debug("election timeout epoch={}", scheduledEpoch);
        cb.run();
    }

    public void stop() {
        synchronized (lock) {
            running.set(false);
            if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
                scheduledFuture.cancel(false);
            }
            scheduledFuture = null;
            currentEpoch = epoch.incrementAndGet();
        }
    }
}
