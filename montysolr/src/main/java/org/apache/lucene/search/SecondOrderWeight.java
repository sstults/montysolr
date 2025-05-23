package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class SecondOrderWeight extends Weight {

    private static final long serialVersionUID = 1999318155593404879L;
    private final Weight innerWeight;
    private final SecondOrderCollector secondOrderCollector;

    public SecondOrderWeight(Weight weight,
                             SecondOrderCollector collector) throws IOException {
        super(weight.getQuery());
        this.innerWeight = weight;
        this.secondOrderCollector = collector;

    }


    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        int docBase = context.docBase;
        int maxRange = docBase + context.reader().maxDoc();
        List<CollectorDoc> hits = secondOrderCollector.getSubReaderResults(docBase, maxRange);
        if (hits == null || hits.size() == 0) return null;
        return new SecondOrderListOfDocsScorer(innerWeight, hits, docBase);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Explanation innerExplanation = innerWeight.explain(context, doc);
        return Explanation.match(innerExplanation.getValue(), "nested, result of", innerExplanation);
    }


    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return innerWeight.isCacheable(ctx);
    }


}