package com.igot.cb.peervalidationcleanup.service;

import java.time.Instant;


public interface PeerValidationCleanupService {

    void runCleanup(Instant jobInstant);
}
