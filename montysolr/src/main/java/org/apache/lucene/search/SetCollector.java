package org.apache.lucene.search;

import java.util.Set;

public interface SetCollector {
    Set<Integer> getHits();
}
