/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import monty.solr.util.MontySolrAbstractTestCase;
import monty.solr.util.MontySolrSetup;
import monty.solr.util.SolrTestSetup;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.AdsConfigHandler;
import org.junit.BeforeClass;


public class TestExtendedConfig extends MontySolrAbstractTestCase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        schemaString = "solr/collection1/conf/schema-minimal.xml";

        configString = "solr/collection1/conf/extended-solrconfig.xml";

        SolrTestSetup.initCore(configString, schemaString);
    }


    public void testExtendedConfig() {
        AdsConfigHandler handler = (AdsConfigHandler) h.getCore().getRequestHandler("/ads-config");
        SolrParams defaults = handler.getParams("defaults");
        assertEquals("false", defaults.get("spellcheck.onlyMorePopular"));
        SolrParams test = handler.getParams("testVal");
        assertEquals("testValue", test.get("testVal"));
    }
}