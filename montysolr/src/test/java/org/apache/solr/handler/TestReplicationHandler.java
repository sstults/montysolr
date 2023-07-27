/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler;

import monty.solr.util.MontySolrSetup;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.MontySolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.StandardDirectoryFactory;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Test for Citation cache; reusing TestReplicationHandler
 */

@Slow
@SuppressSSL     // Currently unknown why SSL does not work with this test
public class TestReplicationHandler extends MontySolrTestCaseJ4 {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String CONF_DIR = "solr"
            + File.separator + "collection1" + File.separator + "conf"
            + File.separator;

    JettySolrRunner masterJetty, slaveJetty, repeaterJetty;
    SolrClient masterClient, slaveClient, repeaterClient;
    SolrInstance master = null, slave = null, repeater = null;

    static String context = "/solr";

    // number of docs to index... decremented for each test case to tell if we accidentally reuse
    // index from previous test method
    static int nDocs = 10;

    @BeforeClass
    public static void beforeClass() {
        makeResourcesVisible(Thread.currentThread().getContextClassLoader(), MontySolrSetup.getMontySolrHome() + "/contrib/adsabs/src/test-files/solr/collection1/conf",
                MontySolrSetup.getMontySolrHome() + "/contrib/adsabs/src/test-files/solr");
        System.setProperty("solr.allow.unsafe.resourceloading", "true");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
//    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
        // For manual testing only
        // useFactory(null); // force an FS factory.
        master = new SolrInstance(createTempDir("solr-instance").toFile(), "master", null);
        master.setUp();
        masterJetty = createJetty(master);
        masterClient = createNewSolrClient(masterJetty.getLocalPort());

        slave = new SolrInstance(createTempDir("solr-instance").toFile(), "slave", masterJetty.getLocalPort());
        slave.setUp();
        slaveJetty = createJetty(slave);
        slaveClient = createNewSolrClient(slaveJetty.getLocalPort());
    }

    public void clearIndexWithReplication() throws Exception {
        if (numFound(query("*:*", masterClient)) != 0) {
            masterClient.deleteByQuery("*:*");
            masterClient.commit();
            // wait for replication to sync & verify
            assertEquals(0, numFound(rQuery(0, "*:*", slaveClient)));
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        masterJetty.stop();
        slaveJetty.stop();
        masterJetty = slaveJetty = null;
        master = slave = null;
        masterClient.close();
        slaveClient.close();
        masterClient = slaveClient = null;
    }

    private static JettySolrRunner createJetty(SolrInstance instance) throws Exception {
        FileUtils.copyFile(new File(MontySolrSetup.getMontySolrHome() + "/contrib/adsabs/src/test-files/solr", "solr.xml"), new File(instance.getHomeDir(), "solr.xml"));
        Properties nodeProperties = new Properties();
        nodeProperties.setProperty("solr.data.dir", instance.getDataDir());
        JettyConfig jettyConfig = JettyConfig.builder().setContext("/solr").setPort(0).build();
        JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), nodeProperties, jettyConfig);
        jetty.start();
        return jetty;
    }

    private static SolrClient createNewSolrClient(int port) {
        try {
            // setup the client...
            final String baseUrl = buildUrl(port) + "/" + DEFAULT_TEST_CORENAME;
            HttpSolrClient client = getHttpSolrClient(baseUrl, 15000, 60000);
            return client;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    int index(SolrClient s, Object... fields) throws Exception {
        SolrInputDocument doc = new SolrInputDocument();
        for (int i = 0; i < fields.length; i += 2) {
            doc.addField((String) (fields[i]), fields[i + 1]);
        }
        return s.add(doc).getStatus();
    }

    NamedList query(String query, SolrClient s) throws SolrServerException, IOException {
        ModifiableSolrParams params = new ModifiableSolrParams();

        params.add("q", query);
        params.add("sort", "id desc");

        QueryResponse qres = s.query(params);
        return qres.getResponse();
    }

    /**
     * will sleep up to 30 seconds, looking for expectedDocCount
     */
    private NamedList rQuery(int expectedDocCount, String query, SolrClient client) throws Exception {
        int timeSlept = 0;
        NamedList res = query(query, client);
        while (expectedDocCount != numFound(res)
                && timeSlept < 30000) {
            log.info("Waiting for " + expectedDocCount + " docs");
            timeSlept += 100;
            Thread.sleep(100);
            res = query(query, client);
        }
        log.info("Waited for {}ms and found {} docs", timeSlept, numFound(res));
        return res;
    }

    private long numFound(NamedList res) {
        return ((SolrDocumentList) res.get("response")).getNumFound();
    }

    private NamedList<Object> getDetails(SolrClient s) throws Exception {


        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("command", "details");
        params.set("_trace", "getDetails");
        params.set("qt", ReplicationHandler.PATH);
        QueryRequest req = new QueryRequest(params);

        NamedList<Object> res = s.request(req);

        assertNotNull("null response from server", res);

        @SuppressWarnings("unchecked") NamedList<Object> details
                = (NamedList<Object>) res.get("details");

        assertNotNull("null details", details);

        return details;
    }

    private NamedList<Object> getCommits(SolrClient s) throws Exception {


        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("command", "commits");
        params.set("_trace", "getCommits");
        params.set("qt", ReplicationHandler.PATH);
        QueryRequest req = new QueryRequest(params);

        NamedList<Object> res = s.request(req);

        assertNotNull("null response from server", res);


        return res;
    }

    private NamedList<Object> getIndexVersion(SolrClient s) throws Exception {

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("command", "indexversion");
        params.set("_trace", "getIndexVersion");
        params.set("qt", ReplicationHandler.PATH);
        QueryRequest req = new QueryRequest(params);

        NamedList<Object> res = s.request(req);

        assertNotNull("null response from server", res);


        return res;
    }

    private NamedList<Object> reloadCore(SolrClient s, String core) throws Exception {

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("action", "reload");
        params.set("core", core);
        params.set("qt", "/admin/cores");
        QueryRequest req = new QueryRequest(params);

        try (HttpSolrClient adminClient = adminClient(s)) {
            NamedList<Object> res = adminClient.request(req);
            assertNotNull("null response from server", res);
            return res;
        }

    }

    private HttpSolrClient adminClient(SolrClient client) {
        String adminUrl = ((HttpSolrClient) client).getBaseURL().replace("/collection1", "");
        return getHttpSolrClient(adminUrl);
    }


    @Test
    public void doTestDetails() throws Exception {
        clearIndexWithReplication();
        {
            NamedList<Object> details = getDetails(masterClient);

            assertEquals("master isMaster?",
                    "true", details.get("isMaster"));
            assertEquals("master isSlave?",
                    "false", details.get("isSlave"));
            assertNotNull("master has master section",
                    details.get("master"));
        }

        // check details on the slave a couple of times before & after fetching
        for (int i = 0; i < 3; i++) {
            NamedList<Object> details = getDetails(slaveClient);

            assertEquals("slave isMaster?",
                    "false", details.get("isMaster"));
            assertEquals("slave isSlave?",
                    "true", details.get("isSlave"));
            assertNotNull("slave has slave section",
                    details.get("slave"));
            // SOLR-2677: assert not false negatives
            Object timesFailed = ((NamedList) details.get("slave")).get(IndexFetcher.TIMES_FAILED);
            // SOLR-7134: we can have a fail because some mock index files have no checksum, will
            // always be downloaded, and may not be able to be moved into the existing index
            assertTrue("slave has fetch error count: " + timesFailed, timesFailed == null || timesFailed.equals("1"));

            if (3 != i) {
                // index & fetch
                index(masterClient, "id", i, "name", "name = " + i);
                masterClient.commit();
                pullFromTo(masterJetty, slaveJetty);
            }
        }
    }


    /**
     * Verify that empty commits and/or commits with openSearcher=false
     * on the master do not cause subsequent replication problems on the slave
     */
    public void testEmptyCommits() throws Exception {
        clearIndexWithReplication();

        // add a doc to master and commit
        index(masterClient, "id", "1", "name", "empty1");
        emptyUpdate(masterClient, "commit", "true");
        // force replication
        pullFromMasterToSlave();
        // verify doc is on slave
        rQuery(1, "name:empty1", slaveClient);
        assertVersions(masterClient, slaveClient);

        // do a completely empty commit on master and force replication
        emptyUpdate(masterClient, "commit", "true");
        pullFromMasterToSlave();

        // add another doc and verify slave gets it
        index(masterClient, "id", "2", "name", "empty2");
        emptyUpdate(masterClient, "commit", "true");
        // force replication
        pullFromMasterToSlave();

        rQuery(1, "name:empty2", slaveClient);
        assertVersions(masterClient, slaveClient);

        // add a third doc but don't open a new searcher on master
        index(masterClient, "id", "3", "name", "empty3");
        emptyUpdate(masterClient, "commit", "true", "openSearcher", "false");
        pullFromMasterToSlave();

        // verify slave can search the doc, but master doesn't
        rQuery(0, "name:empty3", masterClient);
        rQuery(1, "name:empty3", slaveClient);

        // final doc with hard commit, slave and master both showing all docs
        index(masterClient, "id", "4", "name", "empty4");
        emptyUpdate(masterClient, "commit", "true");
        pullFromMasterToSlave();

        String q = "name:(empty1 empty2 empty3 empty4)";
        rQuery(4, q, masterClient);
        rQuery(4, q, slaveClient);
        assertVersions(masterClient, slaveClient);

    }

    @Test
    public void doTestReplicateAfterWrite2Slave() throws Exception {
        clearIndexWithReplication();
        nDocs--;
        for (int i = 0; i < nDocs; i++) {
            index(masterClient, "id", i, "name", "name = " + i);
        }

        invokeReplicationCommand(masterJetty.getLocalPort(), "disableReplication");
        invokeReplicationCommand(slaveJetty.getLocalPort(), "disablepoll");

        masterClient.commit();

        assertEquals(nDocs, numFound(rQuery(nDocs, "*:*", masterClient)));

        // Make sure that both the index version and index generation on the slave is
        // higher than that of the master, just to make the test harder.

        index(slaveClient, "id", 551, "name", "name = " + 551);
        slaveClient.commit(true, true);
        index(slaveClient, "id", 552, "name", "name = " + 552);
        slaveClient.commit(true, true);
        index(slaveClient, "id", 553, "name", "name = " + 553);
        slaveClient.commit(true, true);
        index(slaveClient, "id", 554, "name", "name = " + 554);
        slaveClient.commit(true, true);
        index(slaveClient, "id", 555, "name", "name = " + 555);
        slaveClient.commit(true, true);

        //this doc is added to slave so it should show an item w/ that result
        assertEquals(1, numFound(rQuery(1, "id:555", slaveClient)));

        //Let's fetch the index rather than rely on the polling.
        invokeReplicationCommand(masterJetty.getLocalPort(), "enablereplication");
        invokeReplicationCommand(slaveJetty.getLocalPort(), "fetchindex");

    /*
    //the slave should have done a full copy of the index so the doc with id:555 should not be there in the slave now
    slaveQueryRsp = rQuery(0, "id:555", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(0, slaveQueryResult.getNumFound());

    // make sure we replicated the correct index from the master
    slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());
    
    */
    }

    //Simple function to wrap the invocation of replication commands on the various
    //jetty servers.
    private void invokeReplicationCommand(int pJettyPort, String pCommand) throws IOException {
        String masterUrl = buildUrl(pJettyPort) + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH + "?command=" + pCommand;
        URL u = new URL(masterUrl);
        InputStream stream = u.openStream();
        stream.close();
    }

    //@Test
    public void doTestIndexAndConfigReplication() throws Exception {
        clearIndexWithReplication();
        Random random = random();

        nDocs--;
        for (int i = 0; i < nDocs; i++)
            index(masterClient, "id", i, "name", "name = " + i, "bibcode = " + i, "citation = " + random.nextInt(nDocs));

        masterClient.commit();

        NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
        SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
        assertEquals(nDocs, numFound(masterQueryRsp));

        //get docs from slave and check if number is equal to master
        NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
        SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
        assertEquals(nDocs, numFound(slaveQueryRsp));

        //compare results
        String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
        assertNull(cmp);

        assertVersions(masterClient, slaveClient);

        //start config files replication test
        masterClient.deleteByQuery("*:*");
        masterClient.commit();


        checkForSingleIndex(masterJetty);
        checkForSingleIndex(slaveJetty);

    }


    private String getSlaveDetails(String keyName) throws SolrServerException, IOException {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.QT, "/replication");
        params.set("command", "details");
        QueryResponse response = slaveClient.query(params);
        System.out.println("SHALIN: " + response.getResponse());
        // details/slave/timesIndexReplicated
        NamedList<Object> details = (NamedList<Object>) response.getResponse().get("details");
        NamedList<Object> slave = (NamedList<Object>) details.get("slave");
        Object o = slave.get(keyName);
        return o != null ? o.toString() : null;
    }


    private void checkForSingleIndex(JettySolrRunner jetty) {
        CoreContainer cores = jetty.getCoreContainer();
        Collection<SolrCore> theCores = cores.getCores();
        for (SolrCore core : theCores) {
            String ddir = core.getDataDir();
            CachingDirectoryFactory dirFactory = (CachingDirectoryFactory) core.getDirectoryFactory();
            synchronized (dirFactory) {
                Set<String> livePaths = dirFactory.getLivePaths();
                // one for data, one for hte index under data and one for the snapshot metadata.
                assertEquals(livePaths.toString(), 3, livePaths.size());
                // :TODO: assert that one of the paths is a subpath of hte other
            }
            if (dirFactory instanceof StandardDirectoryFactory) {
                System.out.println(Arrays.asList(new File(ddir).list()));
                assertEquals(Arrays.asList(new File(ddir).list()).toString(), 1, indexDirCount(ddir));
            }
        }
    }

    private int indexDirCount(String ddir) {
        String[] list = new File(ddir).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                File f = new File(dir, name);
                return f.isDirectory() && !SolrSnapshotMetaDataManager.SNAPSHOT_METADATA_DIR.equals(name);
            }
        });
        return list.length;
    }

    private void pullFromMasterToSlave() throws
            IOException {
        pullFromTo(masterJetty, slaveJetty);
    }


    private void assertVersions(SolrClient client1, SolrClient client2) throws Exception {
        NamedList<Object> details = getDetails(client2);
        ArrayList<NamedList<Object>> commits = (ArrayList<NamedList<Object>>) details.get("commits");
        Long maxVersionClient1 = getVersion(client1);
        Long maxVersionClient2 = getVersion(client2);

        if (maxVersionClient1 > 0 && maxVersionClient2 > 0) {
            assertEquals(maxVersionClient1, maxVersionClient2);
        }

        // check vs /replication?command=indexversion call
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", ReplicationHandler.PATH);
        params.set("_trace", "assertVersions");
        params.set("command", "indexversion");
        QueryRequest req = new QueryRequest(params);
        NamedList<Object> resp = client1.request(req);

        Long version = (Long) resp.get("indexversion");
        assertEquals(maxVersionClient1, version);

        // check vs /replication?command=indexversion call
        resp = client2.request(req);
        version = (Long) resp.get("indexversion");
        assertEquals(maxVersionClient2, version);
    }

    private Long getVersion(SolrClient client) throws Exception {
        NamedList<Object> details;
        ArrayList<NamedList<Object>> commits;
        details = getDetails(client);
        commits = (ArrayList<NamedList<Object>>) details.get("commits");
        Long maxVersionSlave = 0L;
        for (NamedList<Object> commit : commits) {
            Long version = (Long) commit.get("indexVersion");
            maxVersionSlave = Math.max(version, maxVersionSlave);
        }
        return maxVersionSlave;
    }

    private void pullFromSlaveToMaster() throws
            IOException {
        pullFromTo(slaveJetty, masterJetty);
    }

    private void pullFromTo(JettySolrRunner from, JettySolrRunner to) throws IOException {
        String masterUrl;
        URL url;
        InputStream stream;
        masterUrl = buildUrl(to.getLocalPort())
                + "/" + DEFAULT_TEST_CORENAME
                + ReplicationHandler.PATH + "?wait=true&command=fetchindex&masterUrl="
                + buildUrl(from.getLocalPort())
                + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH;
        url = new URL(masterUrl);
        stream = url.openStream();
        stream.close();
    }


    @BadApple(bugUrl = "tbd")
    @Test
    public void doTestIndexAndConfigAliasReplication() throws Exception {
        clearIndexWithReplication();

        nDocs--;
        for (int i = 0; i < nDocs; i++)
            index(masterClient, "id", i, "name", "name = " + i);

        masterClient.commit();

        NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
        SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
        assertEquals(nDocs, masterQueryResult.getNumFound());

        //get docs from slave and check if number is equal to master
        NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
        SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");

        assertEquals(nDocs, slaveQueryResult.getNumFound());

        //compare results
        String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
        assertNull(cmp);

        //start config files replication test
        //clear master index
        masterClient.deleteByQuery("*:*");
        masterClient.commit();
        rQuery(0, "*:*", masterClient); // sanity check w/retry

        //change solrconfig on master
        master.copyConfigFile(CONF_DIR + "solrconfig-master1.xml",
                "solrconfig.xml");

        //change schema on master
        master.copyConfigFile(CONF_DIR + "schema-replication2.xml",
                "schema.xml");

        //keep a copy of the new schema
        master.copyConfigFile(CONF_DIR + "schema-replication2.xml",
                "schema-replication2.xml");

        masterJetty.stop();

        masterJetty = createJetty(master);
        masterClient.close();
        masterClient = createNewSolrClient(masterJetty.getLocalPort());

        slave.setTestPort(masterJetty.getLocalPort());
        slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");


        slaveJetty.stop();
        slaveJetty = createJetty(slave);
        slaveClient.close();
        slaveClient = createNewSolrClient(slaveJetty.getLocalPort());

        slaveClient.deleteByQuery("*:*");
        slaveClient.commit();
        rQuery(0, "*:*", slaveClient); // sanity check w/retry

        // record collection1's start time on slave
        final Date slaveStartTime = watchCoreStartAt(slaveClient, 30 * 1000, null);

        //add a doc with new field and commit on master to trigger index fetch from slave.
        index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "n2000");
        masterClient.commit();
        rQuery(1, "newname:n2000", masterClient);  // sanity check

        // wait for slave to reload core by watching updated startTime
        watchCoreStartAt(slaveClient, 30 * 1000, slaveStartTime);

        NamedList masterQueryRsp2 = rQuery(1, "id:2000", masterClient);
        SolrDocumentList masterQueryResult2 = (SolrDocumentList) masterQueryRsp2.get("response");
        assertEquals(1, masterQueryResult2.getNumFound());

        NamedList slaveQueryRsp2 = rQuery(1, "id:2000", slaveClient);
        SolrDocumentList slaveQueryResult2 = (SolrDocumentList) slaveQueryRsp2.get("response");
        assertEquals(1, slaveQueryResult2.getNumFound());

        index(slaveClient, "id", "2001", "name", "name = " + 2001, "newname", "n2001");
        slaveClient.commit();

        slaveQueryRsp = rQuery(1, "id:2001", slaveClient);
        final SolrDocumentList sdl = (SolrDocumentList) slaveQueryRsp.get("response");
        assertEquals(1, sdl.getNumFound());
        final SolrDocument d = sdl.get(0);
        assertEquals("n2001", d.getFieldValue("newname"));

        checkForSingleIndex(masterJetty);
        checkForSingleIndex(slaveJetty);
    }


    private class AddExtraDocs implements Runnable {

        SolrClient masterClient;
        int startId;

        public AddExtraDocs(SolrClient masterClient, int startId) {
            this.masterClient = masterClient;
            this.startId = startId;
        }

        @Override
        public void run() {
            final int totalDocs = TestUtil.nextInt(random(), 1, 10);
            for (int i = 0; i < totalDocs; i++) {
                try {
                    index(masterClient, "id", i + startId, "name", TestUtil.randomSimpleString(random(), 1000, 5000));
                } catch (Exception e) {
                    //Do nothing. Wasn't able to add doc.
                }
            }
            try {
                masterClient.commit();
            } catch (Exception e) {
                //Do nothing. No extra doc got committed.
            }
        }
    }

    /**
     * character copy of file using UTF-8. If port is non-null, will be substituted any time "TEST_PORT" is found.
     */
    private static void copyFile(File src, File dst, Integer port, boolean internalCompression) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(src), StandardCharsets.UTF_8));
        Writer out = new OutputStreamWriter(new FileOutputStream(dst), StandardCharsets.UTF_8);

        for (String line = in.readLine(); null != line; line = in.readLine()) {

            if (null != port)
                line = line.replace("TEST_PORT", port.toString());

            line = line.replace("COMPRESSION", internalCompression ? "internal" : "false");

            out.write(line);
        }
        in.close();
        out.close();
    }

    private UpdateResponse emptyUpdate(SolrClient client, String... params)
            throws SolrServerException, IOException {

        UpdateRequest req = new UpdateRequest();
        req.setParams(params(params));
        return req.process(client);
    }

    /**
     * Polls the SolrCore stats using the specified client until the "startTime"
     * time for collection is after the specified "min".  Will loop for
     * at most "timeout" milliseconds before throwing an assertion failure.
     *
     * @param client  The SolrClient to poll
     * @param timeout the max milliseconds to continue polling for
     * @param min     the startTime value must exceed this value before the method will return, if null this method will return the first startTime value encountered.
     * @return the startTime value of collection
     */
    private Date watchCoreStartAt(SolrClient client, final long timeout,
                                  final Date min) throws InterruptedException, IOException, SolrServerException {
        final long sleepInterval = 200;
        long timeSlept = 0;

        try (HttpSolrClient adminClient = adminClient(client)) {
            SolrParams p = params("action", "status", "core", "collection1");
            while (timeSlept < timeout) {
                QueryRequest req = new QueryRequest(p);
                req.setPath("/admin/cores");
                try {
                    NamedList data = adminClient.request(req);
                    for (String k : new String[]{"status", "collection1"}) {
                        Object o = data.get(k);
                        assertNotNull("core status rsp missing key: " + k, o);
                        data = (NamedList) o;
                    }
                    Date startTime = (Date) data.get("startTime");
                    assertNotNull("core has null startTime", startTime);
                    if (null == min || startTime.after(min)) {
                        return startTime;
                    }
                } catch (SolrException e) {
                    // workarround for SOLR-4668
                    if (500 != e.code()) {
                        throw e;
                    } // else server possibly from the core reload in progress...
                }

                timeSlept += sleepInterval;
                Thread.sleep(sleepInterval);
            }
            fail("timed out waiting for collection1 startAt time to exceed: " + min);
            return min; // compilation neccessity
        }
    }

    private static String buildUrl(int port) {
        return buildUrl(port, context);
    }

    static class SolrInstance {

        private final String name;
        private Integer testPort;
        private final File homeDir;
        private File confDir;
        private File dataDir;

        /**
         * @param homeDir  Base directory to build solr configuration and index in
         * @param name     used to pick which
         *                 "solrconfig-${name}.xml" file gets copied
         *                 to solrconfig.xml in new conf dir.
         * @param testPort if not null, used as a replacement for
         *                 TEST_PORT in the cloned config files.
         */
        public SolrInstance(File homeDir, String name, Integer testPort) {
            this.homeDir = homeDir;
            this.name = name;
            this.testPort = testPort;
        }

        public String getHomeDir() {
            return homeDir.toString();
        }

        public String getSchemaFile() {
            return "schema-batch-provider.xml";
        }

        public String getConfDir() {
            return confDir.toString();
        }

        public String getDataDir() {
            return dataDir.getAbsolutePath();
        }

        public String getSolrConfigFile() {
            return "solrconfig-monty-" + name + ".xml";
        }

        /**
         * If it needs to change
         */
        public void setTestPort(Integer testPort) {
            this.testPort = testPort;
        }

        public void setUp() throws Exception {
            System.setProperty("solr.test.sys.prop1", "propone");
            System.setProperty("solr.test.sys.prop2", "proptwo");

            Properties props = new Properties();
            props.setProperty("name", "collection1");

            writeCoreProperties(homeDir.toPath().resolve("collection1"), props, "TestReplicationHandler");

            dataDir = new File(homeDir + "/collection1", "data");
            confDir = new File(homeDir + "/collection1", "conf");

            homeDir.mkdirs();
            dataDir.mkdirs();
            confDir.mkdirs();

            copyConfigFile(getSolrConfigFile(), "solrconfig.xml");
            copyConfigFile(getSchemaFile(), "schema.xml");
        }

        public void copyConfigFile(String srcFile, String destFile)
                throws IOException {
            copyFile(getFile(srcFile),
                    new File(confDir, destFile),
                    testPort, random().nextBoolean());
        }

    }
}
