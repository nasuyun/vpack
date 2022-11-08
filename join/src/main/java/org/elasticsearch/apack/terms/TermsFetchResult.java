package org.elasticsearch.apack.terms;


import org.elasticsearch.apack.terms.collector.TermsSet;

public final class TermsFetchResult {

    private boolean searchTimedOut;
    private boolean pruned;
    private TermsSet termsSet;
    private long serviceTimeEWMA = -1;
    private int nodeQueueSize = -1;

    public boolean searchTimedOut() {
        return searchTimedOut;
    }

    public void searchTimedOut(boolean searchTimedOut) {
        this.searchTimedOut = searchTimedOut;
    }

    public boolean isPruned() {
        return pruned;
    }

    public void pruned(boolean pruned) {
        this.pruned = pruned;
    }

    public TermsSet terms() {
        return termsSet;
    }

    public void terms(TermsSet termsSet) {
        this.termsSet = termsSet;
    }

    public long serviceTimeEWMA() {
        return this.serviceTimeEWMA;
    }

    public TermsFetchResult serviceTimeEWMA(long serviceTimeEWMA) {
        this.serviceTimeEWMA = serviceTimeEWMA;
        return this;
    }

    public int nodeQueueSize() {
        return this.nodeQueueSize;
    }

    public TermsFetchResult nodeQueueSize(int nodeQueueSize) {
        this.nodeQueueSize = nodeQueueSize;
        return this;
    }

}