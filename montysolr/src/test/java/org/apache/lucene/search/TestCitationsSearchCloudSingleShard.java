package org.apache.lucene.search;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.solr.search.CitationCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.*;

import static org.hamcrest.CoreMatchers.hasItem;


public class TestCitationsSearchCloudSingleShard extends SolrCloudTestCase {

    private static final String COLLECTION = "citations_collection";
    private static final int numShards = 1;
    private static final int numReplicas = 2;
    private static final int nodeCount = numShards * numReplicas;
    private static final boolean debug = false;
    public static final String CITATIONS_CACHE = "citations-cache-from-references";

    private static CloudSolrClient solrClient = null;

    @BeforeClass
    public static void setupCluster() throws Exception {

        configureCluster(nodeCount)
            .addConfig("citation_config",
                java.nio.file.Paths.get("src/test/resources/solr/cloud-conf").toAbsolutePath())
            .configure();

        CollectionAdminRequest.createCollection(COLLECTION, "citation_config", numShards, numReplicas)
                .process(cluster.getSolrClient());

        cluster.waitForActiveCollection(COLLECTION, numShards, numReplicas);
        solrClient = cluster.getSolrClient(COLLECTION);
    }

    @Before
    public void clearCloudCollection() throws Exception {
        assertEquals(0, solrClient.deleteByQuery("*:*").getStatus());
        assertEquals(0, solrClient.commit().getStatus());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        solrClient.close();
        solrClient = null;

        cluster.deleteAllCollections();
        cluster.deleteAllConfigSets();
        cluster.shutdown();
        cluster = null;
    }

    @Test
    public void testBasicSetup() throws Exception {
        // Create and add a document
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "1");
        doc.addField("bibcode", "b1");
        doc.addField("year", "2022");

        UpdateRequest req = new UpdateRequest();
        req.add(doc);
        req.commit(cluster.getSolrClient(), COLLECTION);

        SolrQuery query = new SolrQuery("id:1");
        QueryResponse response = cluster.getSolrClient().query(COLLECTION, query);

        assertEquals(1, response.getResults().size());
        assertEquals(1, response.getResults().get(0).getFieldValue("id"));
    }

    @Test
    public void testCitationsCacheInitialization() throws Exception {
        RefCounted<SolrIndexSearcher> searcher = getSearcherForFirstCore();
        try {
            CitationCache<Object, Integer> cache = (CitationCache<Object, Integer>) searcher.get().getCache(CITATIONS_CACHE);
            assertNotNull("CitationCache not found in searcher", cache);
        } finally {
            searcher.decref();
        }
    }

    @Test
    public void testBasicCitationRelationships() throws Exception {
        // Create test documents with citation relationships
        createDocumentWithReferences("10", "b10", new String[]{"b11", "b12"});
        createDocumentWithReferences("11", "b11", new String[]{"b12"});
        createDocumentWithReferences("12", "b12", new String[]{});

        // Force commit
        cluster.getSolrClient().commit(COLLECTION);

        CitationCache<Object, Integer> cache = null;
        RefCounted<SolrIndexSearcher> searcher = getSearcherForFirstCore();

        try {
            cache = (CitationCache<Object, Integer>) searcher.get().getCache(CITATIONS_CACHE);
            assertNotNull("CitationCache not found in searcher", cache);

            // Verify the citations are in the cache
            int doc10Id = getDocId("10", searcher.get());
            int doc11Id = getDocId("11", searcher.get());
            int doc12Id = getDocId("12", searcher.get());

            // Check references (what doc10 cites)
            int[] doc10Refs = cache.getReferences(doc10Id);
            assertNotNull("References for doc10 should not be null", doc10Refs);
            assertEquals("Doc10 should cite 2 documents", 2, doc10Refs.length);

            // Check citations (what cites doc12)
            int[] doc12Citations = cache.getCitations(doc12Id);
            assertNotNull("Citations for doc12 should not be null", doc12Citations);
            assertEquals("Doc12 should be cited by 2 documents", 2, doc12Citations.length);

            // Check specific citation relationships
            assertThat(Arrays.stream(doc10Refs).boxed().collect(Collectors.toList()), hasItem(doc11Id));
            assertThat(Arrays.stream(doc10Refs).boxed().collect(Collectors.toList()), hasItem(doc12Id));
            assertThat(Arrays.stream(doc12Citations).boxed().collect(Collectors.toList()), hasItem(doc10Id));
            assertThat(Arrays.stream(doc12Citations).boxed().collect(Collectors.toList()), hasItem(doc11Id));
        } finally {
            if (cache != null) {
                cache.clear();
            }
            searcher.decref();
        }
    }

    private void createDocumentWithReferences(String id, String bibcode, String[] references) throws Exception {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField("bibcode", bibcode);
        doc.addField("year", "2022");

        for (String ref : references) {
            doc.addField("reference", ref);
        }

        UpdateRequest req = new UpdateRequest();
        req.add(doc);
        req.commit(cluster.getSolrClient(), COLLECTION);
    }

    private int getDocId(String id, SolrIndexSearcher searcher) throws IOException {
        BytesRefBuilder builder = new BytesRefBuilder();
        LegacyNumericUtils.intToPrefixCoded(Integer.parseInt(id), 0, builder);
        ScoreDoc[] docs = searcher.search(new TermQuery(new Term("id", builder)), 1).scoreDocs;
        assertTrue("Document not found: " + id, docs.length > 0);
        return docs[0].doc;
    }

    private static RefCounted<SolrIndexSearcher> getSearcherForFirstCore() {
        // Get the searcher for the (first) core from the first node's collection, always return a new instance
        return cluster.getJettySolrRunner(0)
            .getCoreContainer()
            .getCores()
            .stream()
            .filter(core -> core.getName().contains(COLLECTION))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Collection not found"))
            .getSearcher();
    }

    private HashMap<Integer, int[]> createRandomDocs(int start, int numDocs) throws Exception {
        Random randomSeed = new Random(42);

        int[] randData = new int[numDocs / 10];
        for (int i = 0; i < randData.length; i++) {
            randData[i] = Math.abs(randomSeed.nextInt(numDocs) - start);
        }

        int x = 0;
        int[][] randi = new int[numDocs - start][];
        for (int i = 0; i < numDocs - start; i++) {
            int howMany = randomSeed.nextInt(6);
            randi[i] = new int[howMany];
            for (int j = 0; j < howMany; j++) {
                if (x >= randData.length) {
                    x = 0;
                }
                randi[i][j] = randData[x++];
            }
        }

        HashMap<Integer, int[]> data = new HashMap<>(randi.length);

        SolrInputDocument doc = new SolrInputDocument();

        for (int k = 0; k < randi.length; k++) {
            doc.clear();
            doc.addField("id", String.valueOf(k + start));
            doc.addField("bibcode", "b" + (k + start));
            if (k % 2 == 0) {
                doc.addField("year", "2000");
            } else {
                doc.addField("year", "1995");
            }
            int[] row = new int[randi[k].length];

            x = 0;
            for (int v : randi[k]) {
                row[x] = v + start;
                doc.addField("reference", "b" + (v + start));
                doc.addField("ireference", String.valueOf(v + start));
                x++;
            }
            UpdateRequest req = new UpdateRequest();
            req.add(doc);
            req.commit(cluster.getSolrClient(), COLLECTION);

            data.put(k + start, row);
            if (debug) System.out.println(doc);
        }

        if (debug) System.out.println("Created random docs: " + start + " - " + numDocs);
        return data;
    }
}
