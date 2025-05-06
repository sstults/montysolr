package org.apache.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.CitationCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.Map.Entry;

/**
 * Test for citation functionality in a multi-shard SolrCloud environment.
 * This test adapts the functionality from TestCitationsSearch to work 
 * with a two-shard SolrCloud configuration.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TestCitationsSearchCloudMultiShard extends SolrTestCaseJ4 {
    
    private MiniSolrCloudCluster solrCloudCluster;
    private CloudSolrClient cloudClient;
    private final boolean debug = false;
    private final int numShards = 2; // Multi-shard configuration with 2 shards
    
    @Before
    public void setUp() throws Exception {
        super.setUp();
        setupCluster();
    }

    @After
    public void tearDown() throws Exception {
        if (cloudClient != null) {
            cloudClient.close();
        }
        if (solrCloudCluster != null) {
            solrCloudCluster.shutdown();
        }
        super.tearDown();
    }

    private void setupCluster() throws Exception {
        // Create a temporary directory for Solr files
        Path tempDir = createTempDir();
        
        // Set up the cluster with multiple nodes - one for each shard
        solrCloudCluster = new MiniSolrCloudCluster(numShards, tempDir,
                MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML, 
                buildJettyConfig());
        
        // Upload the configuration
        String configName = "citation-config";
        Path configDir = getConfigSetPath();
        solrCloudCluster.uploadConfigSet(configDir, configName);
        
        // Create the collection with multiple shards
        CollectionAdminRequest.Create createRequest = CollectionAdminRequest.createCollection(
                "collection1", configName, numShards, 1);
        createRequest.process(solrCloudCluster.getSolrClient());
        
        // Wait for the collection to be available and all shards active
        solrCloudCluster.waitForAllNodes(30);
        
        // Ensure the collection is ready with active shards
        boolean success = false;
        long startTime = System.currentTimeMillis();
        long timeoutMs = TimeUnit.SECONDS.toMillis(30);
        
        while (!success && (System.currentTimeMillis() - startTime) < timeoutMs) {
            try {
                ClusterState clusterState = solrCloudCluster.getSolrClient().getClusterState();
                if (clusterState != null) {
                    DocCollection collection = clusterState.getCollectionOrNull("collection1");
                    if (collection != null) {
                        // Check that all shards are active
                        boolean allShardsActive = true;
                        for (Slice slice : collection.getSlices()) {
                            // Check if this shard has at least one active replica
                            boolean hasActiveReplica = false;
                            for (Replica replica : slice.getReplicas()) {
                                if (replica.isActive(clusterState.getLiveNodes())) {
                                    hasActiveReplica = true;
                                    break;
                                }
                            }
                            if (!hasActiveReplica) {
                                allShardsActive = false;
                                break;
                            }
                        }
                        
                        if (allShardsActive) {
                            success = true;
                            break;
                        }
                    }
                }
                
                // Wait a bit before trying again
                Thread.sleep(500);
            } catch (Exception e) {
                // Continue waiting if there's an error
                Thread.sleep(100);
            }
        }
        
        if (!success) {
            throw new RuntimeException("Timed out waiting for collection to be available and active");
        }
        
        cloudClient = solrCloudCluster.getSolrClient();
        cloudClient.setDefaultCollection("collection1");
    }
    
    private Path getConfigSetPath() throws Exception {
        // Create a temporary directory for the config set
        Path configDir = createTempDir("config");
        
        // Create the solrconfig.xml with citation cache configuration
        String solrConfigXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
                + "<config>\n"
                + "  <luceneMatchVersion>8.0.0</luceneMatchVersion>\n"
                + "  <dataDir>${solr.data.dir:}</dataDir>\n"
                + "  <directoryFactory name=\"DirectoryFactory\" class=\"${solr.directoryFactory:solr.NRTCachingDirectoryFactory}\"/>\n"
                + "  <schemaFactory class=\"ClassicIndexSchemaFactory\"/>\n"
                + "  <codecFactory class=\"solr.SchemaCodecFactory\"/>\n"
                + "  <indexConfig>\n"
                + "    <lockType>native</lockType>\n"
                + "  </indexConfig>\n"
                + "  <updateHandler class=\"solr.DirectUpdateHandler2\">\n"
                + "    <updateLog>\n"
                + "      <str name=\"dir\">${solr.ulog.dir:}</str>\n"
                + "    </updateLog>\n"
                + "  </updateHandler>\n"
                + "  <query>\n"
                + "    <maxBooleanClauses>1024</maxBooleanClauses>\n"
                + "    <filterCache class=\"solr.FastLRUCache\" size=\"512\" initialSize=\"512\" autowarmCount=\"0\"/>\n"
                + "    <queryResultCache class=\"solr.LRUCache\" size=\"512\" initialSize=\"512\" autowarmCount=\"0\"/>\n"
                + "    <documentCache class=\"solr.LRUCache\" size=\"512\" initialSize=\"512\" autowarmCount=\"0\"/>\n"
                + "    <cache name=\"citations-cache-from-references\" class=\"org.apache.solr.search.CitationLRUCache\" size=\"512\" initialSize=\"512\" autowarmCount=\"0\">\n"
                + "      <str name=\"mainCacheKey\">id</str>\n"
                + "      <str name=\"lookupCacheKey\">bibcode</str>\n"
                + "      <str name=\"citationField\">reference</str>\n"
                + "      <str name=\"identifierField\">bibcode</str>\n"
                + "      <str name=\"identifierField\">alternate_bibcode</str>\n"
                + "      <str name=\"uniqueKeyField\">id</str>\n"
                + "      <bool name=\"populateOnStartup\">true</bool>\n"
                + "    </cache>\n"
                + "  </query>\n"
                + "  <requestDispatcher>\n"
                + "    <httpCaching never304=\"true\" />\n"
                + "  </requestDispatcher>\n"
                + "  <requestHandler name=\"/select\" class=\"solr.SearchHandler\" />\n"
                + "  <requestHandler name=\"/update\" class=\"solr.UpdateRequestHandler\" />\n"
                + "</config>";
        
        // Create a schema.xml with citation-related fields
        String schemaXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
                + "<schema name=\"minimal\" version=\"1.6\">\n"
                + "  <types>\n"
                + "    <fieldType name=\"string\" class=\"solr.StrField\" />\n"
                + "    <fieldType name=\"int\" class=\"solr.TrieIntField\"\n"
                + "      precisionStep=\"0\" omitNorms=\"true\" positionIncrementGap=\"0\" />\n"
                + "  </types>\n"
                + "  <fields>\n"
                + "    <field name=\"id\" type=\"int\" indexed=\"true\" stored=\"true\"\n"
                + "      required=\"true\" />\n"
                + "    <field name=\"bibcode\" type=\"string\" indexed=\"true\" stored=\"true\"\n"
                + "      required=\"false\" />\n"
                + "    <field name=\"alternate_bibcode\" type=\"string\" indexed=\"true\" stored=\"true\"\n"
                + "      required=\"false\" multiValued=\"true\"/>\n"  
                + "    <field name=\"reference\" type=\"string\" indexed=\"true\" stored=\"true\" \n"
                + "      multiValued=\"true\"/>\n"
                + "    <field name=\"ireference\" type=\"int\" indexed=\"true\" stored=\"true\" \n"
                + "      multiValued=\"true\"/>\n"
                + "    <field name=\"year\" type=\"string\" indexed=\"true\" stored=\"true\"\n"
                + "      required=\"false\" />\n"
                + "  </fields>\n"
                + "  <uniqueKey>id</uniqueKey>\n"
                + "</schema>";
        
        // Write the config files
        Files.write(configDir.resolve("solrconfig.xml"), solrConfigXml.getBytes());
        Files.write(configDir.resolve("schema.xml"), schemaXml.getBytes());
        
        return configDir;
    }
    
    /**
     * Build a JettyConfig for the MiniSolrCloudCluster
     */
    private org.apache.solr.embedded.JettyConfig buildJettyConfig() {
        return org.apache.solr.embedded.JettyConfig.builder()
                .setContext("/solr")
                .build();
    }

    /**
     * Create random documents with citation relationships
     * In a sharded environment, documents will be distributed across shards
     * based on their ID hash
     */
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

        HashMap<Integer, int[]> data = new HashMap<Integer, int[]>(randi.length);
        List<SolrInputDocument> documents = new ArrayList<>();

        for (int k = 0; k < randi.length; k++) {
            SolrInputDocument doc = new SolrInputDocument();
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
            documents.add(doc);
            data.put(k + start, row);
            
            if (debug) {
                System.out.println("Added document: " + doc);
            }
        }

        // Add documents in batches
        int batchSize = 100;
        for (int i = 0; i < documents.size(); i += batchSize) {
            int end = Math.min(i + batchSize, documents.size());
            cloudClient.add(documents.subList(i, end));
            if (i % 500 == 0) {
                cloudClient.commit();
            }
        }
        cloudClient.commit();

        if (debug) {
            System.out.println("Created random docs: " + start + " - " + numDocs);
        }
        return data;
    }

    /**
     * Test for citation collector functionality in multi-shard SolrCloud
     * This is the main test method that verifies citation functionality works
     * across shards
     */
    @Test
    public void testCitesCollector() throws Exception {
        int maxHits = 1000;
        int maxHitsFound = Float.valueOf(maxHits * 0.3f).intValue();
        
        // Create documents across multiple commits to simulate realistic conditions
        createRandomDocs(0, Float.valueOf(maxHits * 0.4f).intValue());
        cloudClient.commit();

        createRandomDocs(Float.valueOf(maxHits * 0.3f).intValue(), Float.valueOf(maxHits * 0.7f).intValue());
        cloudClient.commit();

        createRandomDocs(Float.valueOf(maxHits * 0.71f).intValue(), Float.valueOf(maxHits * 1.0f).intValue());
        cloudClient.commit();

        createRandomDocs(0, Float.valueOf(maxHits * 0.2f).intValue());
        cloudClient.commit();

        // In a multi-shard environment, we need to check each shard
        Map<Integer, int[]> allReferences = new HashMap<>();
        Map<Integer, int[]> allCitations = new HashMap<>();
        
        // Get cluster state to find all shards and replicas
        ClusterState clusterState = solrCloudCluster.getSolrClient().getClusterState();
        DocCollection collection = clusterState.getCollection("collection1");
        
        // Process each shard to build a complete citation network
        for (Slice slice : collection.getSlices()) {
            Replica replica = slice.getLeader();
            String nodeName = replica.getNodeName();
            String coreName = replica.getCoreName();
            
            JettySolrRunner jetty = findJettyForNode(nodeName);
            if (jetty == null) continue;
            
            CoreContainer coreContainer = jetty.getCoreContainer();
            // Updated core name format based on current Solr version
            SolrCore core = coreContainer.getCore(coreName);
            
            try {
                org.apache.solr.search.SolrIndexSearcher searcher = core.getSearcher().get();
                
                // Get the citation cache for this shard
                final CitationCache cache = (CitationCache) searcher.getCache("citations-cache-from-references");
                if (cache == null) {
                    if (debug) System.out.println("No citation cache found for " + coreName);
                    continue;
                }
                
                SolrCacheWrapper citationsWrapper = new SolrCacheWrapper.CitationsCache(cache);
                SolrCacheWrapper referencesWrapper = new SolrCacheWrapper.ReferencesCache(cache);
        
                // Reconstruct the citation cache for this shard
                HashMap<Integer, int[]> shardReferences = reconstructCitationCache(searcher);
                HashMap<Integer, int[]> shardCitations = invert(shardReferences);
                
                // Merge with the complete set
                allReferences.putAll(shardReferences);
                
                // Merge the citations - need to be careful with merging arrays
                for (Entry<Integer, int[]> entry : shardCitations.entrySet()) {
                    Integer key = entry.getKey();
                    int[] newValues = entry.getValue();
                    
                    if (allCitations.containsKey(key)) {
                        int[] existingValues = allCitations.get(key);
                        int[] mergedValues = new int[existingValues.length + newValues.length];
                        System.arraycopy(existingValues, 0, mergedValues, 0, existingValues.length);
                        System.arraycopy(newValues, 0, mergedValues, existingValues.length, newValues.length);
                        allCitations.put(key, mergedValues);
                    } else {
                        allCitations.put(key, newValues);
                    }
                }
                
                // Simple verification within this shard
                for (Entry<Integer, int[]> es : shardReferences.entrySet()) {
                    int docid = es.getKey();
                    int[] docids = es.getValue();
                    for (int reference : docids) {
                        if (shardCitations.containsKey(reference)) {
                            List<Integer> a = Arrays.stream(shardCitations.get(reference)).boxed().collect(Collectors.toList());
                            List<Integer> b = Arrays.stream(citationsWrapper.getLuceneDocIds(reference)).boxed().collect(Collectors.toList());
                            assertTrue(a.contains(docid));
                            assertTrue(b.contains(docid));
                            assertEquals(a, b);
                        }
                    }
                }
            } finally {
                core.close();
            }
        }
        
        // Now we have the complete citation network across all shards
        // We could run further tests here that verify the citation queries work correctly
        // across the distributed environment
        
        // Example test: verify the size of the citation network
        assertTrue("Expected non-empty references", !allReferences.isEmpty());
        assertTrue("Expected non-empty citations", !allCitations.isEmpty());
        
        if (debug) {
            System.out.println("Total documents with references: " + allReferences.size());
            System.out.println("Total documents with citations: " + allCitations.size());
        }
    }
    
    /**
     * Find the JettySolrRunner instance for a given node name
     */
    private JettySolrRunner findJettyForNode(String nodeName) {
        for (JettySolrRunner jetty : solrCloudCluster.getJettySolrRunners()) {
            if (nodeName.contains(String.valueOf(jetty.getLocalPort()))) {
                return jetty;
            }
        }
        return null;
    }

    /**
     * Reconstruct the citation cache from documents for verification
     */
    private HashMap<Integer, int[]> reconstructCitationCache(org.apache.solr.search.SolrIndexSearcher searcher)
            throws IOException {
        Map<String, Integer> bibcodeToDocid = new HashMap<String, Integer>();
        Map<String, String[]> references = new HashMap<String, String[]>();

        searcher.search(new MatchAllDocsQuery(), new SimpleCollector() {
            private LeafReaderContext context;

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
                this.context = context;
            }

            @Override
            public void collect(int doc) throws IOException {
                Document d = searcher.doc(doc + this.context.docBase);
                bibcodeToDocid.put(d.get("bibcode"), doc + this.context.docBase);
                references.put(d.get("bibcode"), d.getValues("reference"));
            }
        });

        HashMap<Integer, int[]> out = new HashMap<Integer, int[]>();
        for (Entry<String, String[]> es : references.entrySet()) {
            Integer docid = bibcodeToDocid.get(es.getKey());
            Set<Integer> docids = new HashSet<Integer>();
            String[] refs = es.getValue();
            for (int i = 0; refs != null && i < refs.length; i++) {
                if (bibcodeToDocid.get(refs[i]) == null)
                    continue;
                docids.add(bibcodeToDocid.get(refs[i]));
            }
            if (docids.isEmpty()) {
                out.put(docid, new int[0]);
            } else {
                out.put(docid, Arrays.stream(docids.toArray(new Integer[docids.size()])).mapToInt(Integer::intValue).toArray());
            }
        }
        return out;
    }

    /**
     * Invert the citation map to get a map of citations to citing documents
     */
    private HashMap<Integer, int[]> invert(HashMap<Integer, int[]> cites) {
        HashMap<Integer, List<Integer>> result = new HashMap<Integer, List<Integer>>(cites.size());
        for (Entry<Integer, int[]> e : cites.entrySet()) {
            for (int paperId : e.getValue()) {
                if (!result.containsKey(paperId)) {
                    result.put(paperId, new ArrayList<Integer>());
                }
                result.get(paperId).add(e.getKey());
            }
        }
        HashMap<Integer, int[]> out = new HashMap<Integer, int[]>();
        for (Entry<Integer, List<Integer>> e : result.entrySet()) {
            List<Integer> list = e.getValue();
            int[] ret = new int[list.size()];
            for (int i = 0; i < ret.length; i++)
                ret[i] = list.get(i);
            out.put(e.getKey(), ret);
        }
        return out;
    }
}
