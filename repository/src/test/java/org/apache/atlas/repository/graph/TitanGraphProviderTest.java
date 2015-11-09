/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.BaseTest;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * TitanGraphProvider unit tests.
 */
public class TitanGraphProviderTest extends BaseTest {

    @Test
    public void testGetConfiguration() throws Exception {
        TestTitanGraphProvider provider = new TestTitanGraphProvider();
        Configuration config = provider.getConfiguration();

        // assert properties from config file
        assertEquals("berkeleyje", config.getString("storage.backend"));

        // assert property which is a comma delimited list
        List hosts = config.getList("storage.hostname");
        assertEquals(3, hosts.size());

        assertEquals("host1", hosts.get(0));
        assertEquals("host2", hosts.get(1));
        assertEquals("host3", hosts.get(2));
    }

    private static class TestTitanGraphProvider extends TitanGraphProvider {
        private Configuration configuration;

        @Override
        PropertiesConfiguration getApplicationProperties() throws AtlasException {
            PropertiesConfiguration config = super.getApplicationProperties();
            config.setProperty("atlas.graph.storage.hostname", "host1,host2,host3");
            return config;
        }
    }
}
