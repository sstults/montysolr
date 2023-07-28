package monty.solr.util;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.TestHarness;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.security.ProtectionDomain;

public class SolrTestSetup extends SolrTestCaseJ4 {

    public static void initCore(String config, String schema) throws Exception {
        // Set required system variables
        SolrTestCaseJ4.configString = null;
        SolrTestCaseJ4.initCore();

        // Create a resource loader + config that use our test class' resource loader
        System.setProperty("solr.allow.unsafe.resourceloading", "true");
        Path solrPath = Files.createTempDirectory("montysolr");

        // Add repo root + ADS solr conf to classpath
        Path solrConfPath = Paths.get("deploy", "adsabs", "server", "solr", "collection1", "conf");
        URLClassLoader extendedLoader = URLClassLoader.newInstance(
                new URL[] {getRepoUrl(), getRepoUrl(solrConfPath)},
                SolrTestSetup.class.getClassLoader());

        SolrResourceLoader loader = new SolrResourceLoader(solrPath,
                extendedLoader);
        SolrConfig solrConfig = SolrConfig.readFromResourceLoader(loader, config);

        // Perform the rest of createCore
        SolrTestCaseJ4.h = new TestHarness(
                TestHarness.buildTestNodeConfig(loader),
                new TestHarness.TestCoresLocator(SolrTestCaseJ4.DEFAULT_TEST_CORENAME,
                        solrPath.toString(), config, schema)
        );
        SolrTestCaseJ4.h.coreName = coreName;
        /*
        SolrTestCaseJ4.h = new TestHarness(coreName,
                hdfsDataDir == null ? initCoreDataDir.getAbsolutePath() : hdfsDataDir,
                solrConfig,
                schema);
         */

        SolrTestCaseJ4.lrf = h.getRequestFactory("", 0, 20, CommonParams.VERSION, "2.2");

        // Set remaining variables
        SolrTestCaseJ4.configString = config;
        SolrTestCaseJ4.schemaString = schema;
        SolrTestCaseJ4.testSolrHome = solrPath;
    }

    public static URL getRepoUrl() throws URISyntaxException, MalformedURLException {
        Class<SolrTestSetup> clazz = SolrTestSetup.class;
        ProtectionDomain protectionDomain = clazz.getProtectionDomain();
        CodeSource codeSource = protectionDomain.getCodeSource();

        Path filePath = Paths.get(codeSource.getLocation().toURI());
        while (!filePath.resolve(".gitignore").toFile().exists()) {
            filePath = filePath.getParent();
        }

        return filePath.toUri().toURL();
    }

    public static URL getRepoUrl(Path subpath) throws URISyntaxException, MalformedURLException {
        return Paths.get(getRepoUrl().toURI()).resolve(subpath).toUri().toURL();
    }
}
