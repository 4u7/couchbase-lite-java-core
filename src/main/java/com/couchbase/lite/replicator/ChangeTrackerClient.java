package com.couchbase.lite.replicator;

import com.couchbase.lite.internal.InterfaceAudience;

import java.util.Map;

/**
 * @exclude
 */
@InterfaceAudience.Private
public interface ChangeTrackerClient {
    void changeTrackerReceivedChange(Map<String, Object> change);

    void changeTrackerStopped(ChangeTracker tracker);

    void changeTrackerFinished(ChangeTracker tracker);

    void changeTrackerCaughtUp();
}
