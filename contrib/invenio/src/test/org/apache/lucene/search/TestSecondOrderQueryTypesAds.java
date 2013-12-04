package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import monty.solr.util.MontySolrAbstractLuceneTestCase;
import monty.solr.util.MontySolrAbstractTestCase;
import monty.solr.util.MontySolrSetup;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MockIndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.SecondOrderCollector.FinalValueType;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.FieldProperties;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.search.CitationLRUCache;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
//import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.junit.BeforeClass;

@SuppressCodecs("SimpleText")
public class TestSecondOrderQueryTypesAds extends MontySolrAbstractTestCase {

	public String getSchemaFile() {
		return MontySolrSetup.getMontySolrHome()
		    + "/contrib/adsabs/src/test-files/solr/collection1/conf/"
		    + "schema-citations-transformer.xml";
	}

	public String getSolrConfigFile() {
		return MontySolrSetup.getMontySolrHome()
		    + "/contrib/adsabs/src/test-files/solr/collection1/conf/"
		    + "citation-cache-solrconfig.xml";
	}

	private SolrQueryRequest tempReq;

	@Override
	public void setUp() throws Exception {
		super.setUp();
		addDocs();
		tempReq = req("test");
	}

	@Override
	public void tearDown() throws Exception {
		if (tempReq != null) {
			tempReq.close();
		}
		tempReq = null;
		super.tearDown();
	}

	private void addDocs() throws IOException {

		assertU(adoc("id", "0", "bibcode", "b0", "boost_const", "1.0f", "boost_2",
		    "0.5f", "boost_1", "0.1f", "date", "1966-01-01T00:00:00Z"));
		assertU(adoc("id", "1", "bibcode", "b1", "boost_const", "1.0f", "boost_2",
		    "0.5f", "reference", "b2", "reference", "b3", "reference", "b4",
		    "reference", "b5", "boost_1", "0.1f", "date", "1966-01-02T00:00:00Z"));
		assertU(adoc("id", "2", "bibcode", "b2", "boost_const", "1.0f", "boost_2",
		    "0.2f", "boost_1", "0.1f", "date", "1966-01-03T00:00:00Z"));
		assertU(adoc("id", "3", "bibcode", "b3", "boost_const", "1.0f", "boost_2",
		    "0.3f", "boost_1", "0.9f", "date", "1966-01-03T01:00:00Z"));
		assertU(adoc("id", "4", "bibcode", "b4", "boost_const", "1.0f", "boost_2",
		    "0.1f", "boost_1", "0.1f", "reference", "b100", "date",
		    "1966-01-03T01:01:00Z"));
		assertU(adoc("id", "5", "bibcode", "b5", "boost_const", "1.0f", "boost_2",
		    "0.8f", "boost_1", "0.0f", "date", "1966-01-03T01:01:01Z"));

		assertU(commit());

		assertU(adoc("id", "6", "bibcode", "b6", "boost_const", "1.0f", "boost_2",
		    "0.1f", "boost_1", "0.5f", "reference", "b5"));
		assertU(adoc("id", "7", "bibcode", "b7", "boost_const", "1.0f", "boost_2",
		    "0.1f", "boost_1", "0.9f"));
		assertU(adoc("id", "8", "bibcode", "b8", "boost_const", "1.0f", "boost_2",
		    "0.1f", "boost_1", "0.9f", "reference", "b9"));
		assertU(adoc("id", "9", "bibcode", "b9", "boost_const", "1.0f", "boost_2",
		    "0.1f", "reference", "b10", "boost_1", "0.9f"));
		assertU(adoc("id", "10", "bibcode", "b10", "boost_const", "1.0f",
		    "boost_2", "0.5f", "boost_1", "0.1f", "reference", "b9"));
		assertU(adoc("id", "11", "bibcode", "b11", "boost_const", "1.0f",
		    "boost_2", "0.5f", "boost_1", "0.1f", "reference", "b9", "reference",
		    "b6"));

		assertU(commit()); // close the writer, create a new segment

		assertU(adoc("xkw", "x", "xka", "b", "id", "16", "bibcode", "b16",
		    "reference", "b17", "reference", "b18", "reference", "b20", "boost_1",
		    "0.9f")); // links: 1
		assertU(adoc("xkw", "x", "xka", "a", "id", "17", "bibcode", "b17",
		    "reference", "b16", "reference", "b18", "reference", "b20", "boost_1",
		    "0.7f")); // links: 2
		assertU(adoc("xkw", "x", "xka", "b", "id", "18", "bibcode", "b18",
		    "reference", "b20", "boost_1", "0.5f")); // links: 3
		assertU(adoc("xkw", "x", "xka", "b", "id", "19", "bibcode", "b19",
		    "reference", "b17", "reference", "b18", "reference", "b20", "boost_1",
		    "0.3f")); // links: 0
		assertU(adoc("xkw", "x", "xka", "b", "id", "20", "bibcode", "b20",
		    "reference", "b20", "boost_1", "0.1f")); // links: 5

		assertU(commit());
	}

	public void testADSOperators() throws Exception {
  	
  	
  	final CitationLRUCache cache = (CitationLRUCache) tempReq.getSearcher().getCache("citations-cache");
		
		
		CacheWrapper citationsWrapper = new SecondOrderCollectorCacheWrapper() {
			@Override
			public AtomicReader getAtomicReader() {
				return tempReq.getSearcher().getAtomicReader();
			}
			@Override
		  public int[] getLuceneDocIds(int sourceDocid) {
			  return cache.getCitations(sourceDocid);
		  }
			
			@Override
			public int getLuceneDocId(int sourceDocid, Object sourceValue) {
			  return (Integer) cache.get(sourceValue);
		  }
			@Override
      public int internalHashCode() {
        return cache.hashCode();
      }
			@Override
      public String internalToString() {
        return cache.name();
      }
		};
		
		CacheWrapper referencesWrapper = new SecondOrderCollectorCacheWrapper() {
			@Override
			public AtomicReader getAtomicReader() {
				return tempReq.getSearcher().getAtomicReader();
			}
			@Override
		  public int[] getLuceneDocIds(int sourceDocid) {
			  return cache.getReferences(sourceDocid);
		  }
			@Override
			public int getLuceneDocId(int sourceDocid, Object sourceValue) {
			  Object v = cache.get(sourceValue);
			  if (v == null) {
			  	return -1;
			  }
			  return (Integer) v;
		  }
			@Override
      public int internalHashCode() {
        return cache.hashCode();
      }
			@Override
      public String internalToString() {
        return cache.name();
      }
		};
		
  	
  	
		// expecting 4 results with various order, simply based on the boost factor
  	testQ2("id:1", new SecondOrderCollectorOperatorExpertsCiting(referencesWrapper, "boost_const"),
  			Arrays.asList(2, 3, 4, 5));
  	testQ2("id:1", new SecondOrderCollectorOperatorExpertsCiting(referencesWrapper, "boost_1"),
  			Arrays.asList(3, 2, 4, 5));
  	testQ2("id:1", new SecondOrderCollectorOperatorExpertsCiting(referencesWrapper, "boost_2"),
  			Arrays.asList(5, 3, 2, 4));
  	


    // 5 is referenced from two docs, the rest only by one doc
    testQ2("id:1 OR id:6", new SecondOrderCollectorOperatorExpertsCiting(referencesWrapper, "boost_const"),
  			Arrays.asList(5,2,3,4));
    testQ2("id:1 OR id:6", new SecondOrderCollectorOperatorExpertsCiting(referencesWrapper, "boost_1"),
  			Arrays.asList(5,3,2,4));
    testQ2("id:1 OR id:6", new SecondOrderCollectorOperatorExpertsCiting(referencesWrapper, "boost_2"),
  			Arrays.asList(5,3,2,4));
  	
  	


    
  	
  	
  	// the most cited papers (the score comes from the source that cites them; so there is no
  	// change in order
  	testQ2("id:9", new SecondOrderCollectorCitingTheMostCited(citationsWrapper, "boost_const"),
  			Arrays.asList(8,10,11));
  	testQ2("id:9", new SecondOrderCollectorCitingTheMostCited(citationsWrapper, "boost_1"),
  			Arrays.asList(8,10,11));
  	testQ2("id:9", new SecondOrderCollectorCitingTheMostCited(citationsWrapper, "boost_2"),
  			Arrays.asList(8,10,11));
  	
  	// 11 is referenced twice (but we should see no change in order)
  	testQ2("id:6 OR id:9", new SecondOrderCollectorCitingTheMostCited(citationsWrapper, "boost_const"),
  			Arrays.asList(8,10,11));
  	testQ2("id:6 OR id:9", new SecondOrderCollectorCitingTheMostCited(citationsWrapper, "boost_1"),
  			Arrays.asList(8,10,11));
  	testQ2("id:6 OR id:9", new SecondOrderCollectorCitingTheMostCited(citationsWrapper, "boost_2"),
  			Arrays.asList(8,10,11));
  	
  	
    
    
    
    // ADS Classic scoring formula
  	testQ2("*:*", new SecondOrderCollectorAdsClassicScoringFormula(citationsWrapper, "boost_const"),
  			Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 16, 17, 18, 19, 20));
  	testQ2("*:*", new SecondOrderCollectorAdsClassicScoringFormula(citationsWrapper, "boost_1"),
  			Arrays.asList(3, 7, 8, 9, 16, 17, 6, 18, 19, 0, 1, 2, 4, 10, 11, 20, 5));
  	testQ2("*:*", new SecondOrderCollectorAdsClassicScoringFormula(citationsWrapper, "boost_2"),
  			Arrays.asList(5, 0, 1, 10, 11, 3, 2, 4, 6, 7, 8, 9, 16, 17, 18, 19, 20));
  	
   // topN()
  	testQ2((Query) new SecondOrderQuery(new MatchAllDocsQuery(), 
  			new SecondOrderCollectorAdsClassicScoringFormula(citationsWrapper, "boost_2")), 
  			new SecondOrderCollectorTopN(2, true),
  			Arrays.asList(5,0));
  	testQ2("*:*", new SecondOrderCollectorTopN(2, true),
  			Arrays.asList(0,1));
  	
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
  public void testFinalValueAlgorithms() throws Exception {
    
    // various algorithms for compacting the hits
  	
  	
  	testQ3(FinalValueType.ABS_COUNT,
  			new HashMap() {{
  	  		put(0, new Float[] {1.0f, 1.0f});
  	  		put(1, new Float[] {5.0f});
  	  	}},
  	  	Arrays.asList(0,1),
  			Arrays.asList(2.0f, 1f)
  			);
  	
  	testQ3(FinalValueType.ABS_COUNT_NORM,
  			new HashMap() {{
  	  		put(0, new Float[] {1.0f, 1.0f});
  	  		put(1, new Float[] {5.0f});
  	  	}},
  	  	Arrays.asList(0,1),
  			Arrays.asList(1.0f, 0.5f)
  			);
  	
  	testQ3(FinalValueType.MIN_VALUE,
  			new HashMap() {{
  	  		put(0, new Float[] {1.0f, 1.0f, 0.01f});
  	  		put(1, new Float[] {5.0f, 0.1f});
  	  	}},
  	  	Arrays.asList(1, 0),
  			Arrays.asList(0.1f, 0.01f)
  			);
  	
  	testQ3(FinalValueType.MAX_VALUE,
  			new HashMap() {{
  	  		put(0, new Float[] {1.0f, 1.0f, 0.01f});
  	  		put(1, new Float[] {5.0f, 0.1f});
  	  	}},
  	  	Arrays.asList(1, 0),
  			Arrays.asList(5f, 1f)
  			);
  	
  	testQ3(FinalValueType.GEOM_MEAN,
  			new HashMap() {{
  	  		put(0, new Float[] {1.0f, 1.0f, 0.01f});
  	  		put(1, new Float[] {1.0f, 1.0f, 0.011f});
  	  	}},
  	  	Arrays.asList(1, 0),
  			Arrays.asList(0.222f, 0.215f)
  			);
  	
  	testQ3(FinalValueType.GEOM_MEAN_NORM,
  			new HashMap() {{
  				put(0, new Float[] {1.0f, 1.0f, 0.01f});
  	  		put(1, new Float[] {1.0f, 1.0f, 0.011f});
  	  	}},
  	  	Arrays.asList(1, 0),
  			Arrays.asList(1.0f, 0.969f)
  			);
  	
  	testQ3(FinalValueType.ARITHM_MEAN,
  			new HashMap() {{
  				put(0, new Float[] {1.0f, 1.0f, 0.01f});
  	  		put(1, new Float[] {1.0f, 1.0f, 0.011f});
  	  	}},
  	  	Arrays.asList(1, 0),
  			Arrays.asList(0.6703f, 0.67f)
  			);
  	
  	testQ3(FinalValueType.ARITHM_MEAN_NORM,
  			new HashMap() {{
  				put(0, new Float[] {1.0f, 1.0f, 0.01f});
  	  		put(1, new Float[] {1.0f, 1.0f, 0.011f});
  	  	}},
  	  	Arrays.asList(1, 0),
  			Arrays.asList(1.0f, 0.9995f)
  			);
  	
  	testQ3(FinalValueType.AGRESTI_COULL,
  			new HashMap() {{
  				put(0, new Float[] {2f});
  	  		put(1, new Float[] {1.0f, 1.0f});
  	  	}},
  	  	Arrays.asList(1, 0),
  			Arrays.asList(-0.082f, -0.399f)
  			);
    
  }

	@SuppressWarnings("unchecked")
	private void testQ2(Object firstQuery, SecondOrderCollector collector,
	    List<Integer> expectedIds) throws ParseException, IOException {
		SolrQueryRequest r = req("test");
		try {

			SolrIndexSearcher searcher = r.getSearcher();

			Query q;
			if (firstQuery instanceof String) {
				QParserPlugin qParser = r.getCore().getQueryPlugin("lucene");
				QParser qp = qParser.createParser((String) firstQuery, null,
				    r.getParams(), r);
				q = qp.parse();
			} else {
				q = (Query) firstQuery;
			}

			final ArrayList<ScoreDoc> results = new ArrayList<ScoreDoc>();

			searcher.search(new SecondOrderQuery(q, null, collector),
			    new Collector() {

				    private int docBase;
				    private Scorer scorer;
				    private AtomicReader reader;

				    @Override
				    public void setScorer(Scorer scorer) throws IOException {
					    this.scorer = scorer;
				    }

				    @Override
				    public void setNextReader(AtomicReaderContext context)
				        throws IOException {
					    docBase = context.docBase;
					    reader = context.reader();
				    }

				    @Override
				    public void collect(int doc) throws IOException {
					    Document d = reader.document(doc);
					    String idValue = d.get("id");
					    // store 'id' instead of docid
					    results.add(new ScoreDoc(Integer.parseInt(idValue), scorer
					        .score()));
				    }

				    @Override
				    public boolean acceptsDocsOutOfOrder() {
					    return false;
				    }
			    });

			Collections.sort(results, new Comparator() {
				@Override
				public int compare(Object o1, Object o2) {
					float a = ((ScoreDoc) o1).score;
					float b = ((ScoreDoc) o2).score;
					return a < b ? 1 : a > b ? -1 : 0;
				}
			});

			int[] resultIds = new int[results.size()];
			int i = 0;
			for (ScoreDoc sd : results) {
				resultIds[i] = sd.doc;
				i++;
			}
			int[] arrExpected = new int[expectedIds.size()];
			i = 0;
			for (Integer u : expectedIds) {
				arrExpected[i] = u;
				i++;
			}
			//System.out.println("expected:" + Arrays.toString(arrExpected));
			;
			//System.out.println("results:" + Arrays.toString(resultIds));
			;
			//System.out.println(results);
			assertArrayEquals(arrExpected, resultIds);
		} finally {
			r.close();
		}
	}

	@SuppressWarnings("unchecked")
  private void testQ3(FinalValueType finalType,
  		Map<Integer, Float[]> mockValues,
  		List<Integer> expectedIds,
  		List<Float> expectedScores
  		) throws ParseException, IOException {
  	SolrQueryRequest r = req("test");
  	try {
  		
  	final FinalValueType testFinalType = finalType;
  	final Iterator<Entry<Integer, Float[]>> es = mockValues.entrySet().iterator();
  	
  	SolrIndexSearcher searcher = r.getSearcher();
  	
  	
  	final ArrayList<ScoreDoc> results = new ArrayList<ScoreDoc>();
		
  	searcher.search(new SecondOrderQuery(new MatchAllDocsQuery(), null, new AbstractSecondOrderCollector() {
			private int i = 0;
			@Override
			public void setNextReader(AtomicReaderContext context) throws IOException {
				setFinalValueType(testFinalType);
			}
			
			@Override
			public void collect(int doc) throws IOException {
				if (es.hasNext()) {
					Entry<Integer, Float[]> nextItem = es.next();
					Integer docid = nextItem.getKey();
					for (Float f: nextItem.getValue()) {
						hits.add(new CollectorDoc(docid, f, -1, 1));
					}
				}
			}
			
			@Override
			public boolean acceptsDocsOutOfOrder() {
				return false;
			}
		}),
  			new Collector() { // this one collects results
					
					private int docBase;
					private Scorer scorer;
					private AtomicReader reader;

					@Override
					public void setScorer(Scorer scorer) throws IOException {
						this.scorer = scorer;
					}
					@Override
					public void setNextReader(AtomicReaderContext context) throws IOException {
						docBase = context.docBase;
						reader = context.reader();
					}
					
					@Override
					public void collect(int doc) throws IOException {
						Document d = reader.document(doc);
						String idValue = d.get("id");
						// store 'id' instead of docid
						results.add(new ScoreDoc(Integer.parseInt(idValue), scorer.score()));
					}
					
					@Override
					public boolean acceptsDocsOutOfOrder() {
						return false;
					}
				});
  	
  	Collections.sort(results, new Comparator() {
			@Override
      public int compare(Object o1, Object o2) {
				float a = ((ScoreDoc) o1).score;
				float b = ((ScoreDoc) o2).score;
				return a < b ? 1
		         : a > b ? -1
		         : 0;
      }
  	});
  	
  	int[] resultIds = new int[results.size()];
		int i = 0;
		for (ScoreDoc sd : results) {
			resultIds[i] = sd.doc;
			i++;
		}
  	int[] arrExpected = new int[expectedIds.size()];
		i = 0;
		for (Integer v : expectedIds) {
			arrExpected[i] = v;
			i++;
		}
		
		
  	
  	float[] resultScores = new float[results.size()];
  	i = 0;
  	for (ScoreDoc sd: results) {
  		resultScores[i] = sd.score;
  		i++;
  	}
  	float[] scoresExpected = new float[expectedScores.size()];
  	i = 0;
  	for (Float x: expectedScores) {
  		scoresExpected[i] = x;
  		i++;
  	}
  	
  	//System.out.println("expected (ids):" + Arrays.toString(arrExpected));
		//System.out.println("results:" + Arrays.toString(resultIds));
		
		
  	//System.out.println("expected (scores):" + Arrays.toString(scoresExpected));;
  	//System.out.println("results:" + Arrays.toString(resultScores));;
  	//System.out.println(results);
  	
  	assertArrayEquals(scoresExpected , resultScores, 0.001f);
  	assertArrayEquals(arrExpected, resultIds);
  	}
  	finally {
  		r.close();
  	}
  }

	private int[] getIds(ScoreDoc[] docs) {
		int[] out = new int[docs.length];
		int i = 0;
		for (ScoreDoc d : docs) {
			out[i++] = d.doc;
		}
		return out;
	}

	private float[] getScores(ScoreDoc[] docs) {
		float[] out = new float[docs.length];
		int i = 0;
		for (ScoreDoc d : docs) {
			out[i++] = d.score;
		}
		return out;
	}

	// Uniquely for Junit 3
	public static junit.framework.Test suite() {
		return new junit.framework.JUnit4TestAdapter(
		    TestSecondOrderQueryTypesAds.class);
	}

	/*
	 * Test docs are in the given order (the order is not important where the
	 * score is the same [for the given subset]
	 */
	private void testDocOrder(ScoreDoc[] docs, int... expected) {
		float lastScore = docs[0].score;

		int[] ids = getIds(docs);
		float[] scores = getScores(docs);
		HashMap<Float, List<Integer>> scoreMap = new HashMap<Float, List<Integer>>();
		for (int i = 0; i < ids.length; i++) {
			float s = scores[i];
			if (!scoreMap.containsKey(s)) {
				scoreMap.put(s, new ArrayList<Integer>());
			}
			scoreMap.get(s).add(ids[i]);
		}

		for (int i = 0; i < expected.length; i++) {
			ScoreDoc doc = docs[i];
			System.out.println(doc);

			if (lastScore != doc.score) {
				scoreMap.remove(lastScore);
				lastScore = doc.score;
			}

			if (!scoreMap.get(lastScore).contains((Integer) doc.doc)) {
				fail("The document on position " + i + " is not " + expected[i]
				    + ", we want: " + doc.doc);
			}

		}
	}
}