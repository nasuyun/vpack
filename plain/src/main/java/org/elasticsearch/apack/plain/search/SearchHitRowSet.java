
package org.elasticsearch.apack.plain.search;

import org.elasticsearch.apack.plain.PlainIllegalArgumentException;
import org.elasticsearch.apack.plain.schema.Cursor;
import org.elasticsearch.apack.plain.search.extractor.HitExtractor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.*;

/**
 * Extracts rows from an array of {@link SearchHit}.
 */
class SearchHitRowSet extends ResultRowSet<HitExtractor> {
    private final SearchHit[] hits;
    private final Cursor cursor;
    private final Set<String> innerHits = new LinkedHashSet<>();
    private final String innerHit;

    private final int size;
    private final int[] indexPerLevel;
    private int row = 0;

    SearchHitRowSet(List<HitExtractor> exts, BitSet mask, SearchHit[] hits, int limit, String scrollId) {
        super(exts, mask);

        this.hits = hits;

         // Since the results might contain nested docs, the iteration is similar to that of Aggregation
         // namely it discovers the nested docs and then, for iteration, increments the deepest level first
         // and eventually carries that over to the top level

        String innerHit = null;
        for (HitExtractor ex : exts) {
            if (ex.hitName() != null) {
                innerHits.add(ex.hitName());
                if (innerHit == null) {
                    innerHit = ex.hitName();
                }
            }
        }

        int sz = hits.length;

        int maxDepth = 0;
        if (!innerHits.isEmpty()) {
            if (innerHits.size() > 1) {
                throw new PlainIllegalArgumentException("Multi-nested docs not yet supported {}", innerHits);
            }
            maxDepth = 1;

            sz = 0;
            for (SearchHit hit : hits) {
                for (String ih : innerHits) {
                    SearchHits sh = hit.getInnerHits().get(ih);
                    if (sh != null) {
                        sz += sh.getHits().length;
                    }
                }
            }
        }
        // page size
        size = limit < 0 ? sz : Math.min(sz, limit);
        indexPerLevel = new int[maxDepth + 1];
        this.innerHit = innerHit;

        if (scrollId == null) {
            /* SearchResponse can contain a null scroll when you start a
             * scroll but all results fit in the first page. */
            cursor = Cursor.EMPTY;
        } else {
            // compute remaining limit (only if the limit is specified - that is, positive).
            int remainingLimit = limit < 0 ? limit : limit - size;
            // if the computed limit is zero, or the size is zero it means either there's nothing left or the limit has been reached
            if (size == 0 || remainingLimit == 0) {
                cursor = Cursor.EMPTY;
            } else {
                cursor = new ScrollCursor(scrollId, extractors(), mask, remainingLimit);
            }
        }
    }
    
    protected boolean isLimitReached() {
        return cursor == Cursor.EMPTY;
    }

    @Override
    protected Object extractValue(HitExtractor e) {
        int extractorLevel = e.hitName() == null ? 0 : 1;

        SearchHit hit = null;
        SearchHit[] sh = hits;
        for (int lvl = 0; lvl <= extractorLevel ; lvl++) {
            // TODO: add support for multi-nested doc
            if (hit != null) {
                SearchHits innerHits = hit.getInnerHits().get(innerHit);
                sh = innerHits == null ? SearchHits.EMPTY : innerHits.getHits();
            }
            hit = sh[indexPerLevel[lvl]];
        }

        return e.extract(hit);
    }

    @Override
    protected boolean doHasCurrent() {
        return row < size;
    }

    @Override
    protected boolean doNext() {
        if (row < size - 1) {
            row++;
            // increment last row
            indexPerLevel[indexPerLevel.length - 1]++;
            // then check size
            SearchHit[] sh = hits;
            for (int lvl = 0; lvl < indexPerLevel.length; lvl++) {
                if (indexPerLevel[lvl] == sh.length) {
                    // reset the current branch
                    indexPerLevel[lvl] = 0;
                    // bump the parent - if it's too big it, the loop will restart again from that position
                    indexPerLevel[lvl - 1]++;
                    // restart the loop
                    lvl = 0;
                    sh = hits;
                }
                else {
                    SearchHit h = sh[indexPerLevel[lvl]];
                    // TODO: improve this for multi-nested responses
                    String path = lvl == 0 ? innerHit : null;
                    if (path != null) {
                        SearchHits innerHits = h.getInnerHits().get(path);
                        sh = innerHits == null ? SearchHits.EMPTY : innerHits.getHits();
                    }
                }
            }

            return true;
        }
        return false;
    }

    @Override
    protected void doReset() {
        row = 0;
        Arrays.fill(indexPerLevel, 0);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Cursor nextPageCursor() {
        return cursor;
    }
}