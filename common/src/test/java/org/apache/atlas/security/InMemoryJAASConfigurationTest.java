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

package org.apache.atlas.security;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.hadoop.util.StringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;


//Unstable test. Disabling
@Test(enabled=false)
public class InMemoryJAASConfigurationTest {

    private static final String ATLAS_JAAS_PROP_FILE = "atlas-jaas.properties";

    @BeforeMethod
    protected void setUp() throws Exception {
        try {
            InMemoryJAASConfiguration.init(ATLAS_JAAS_PROP_FILE);
        } catch(Throwable t) {
            fail("InMemoryJAASConfiguration.init() is not expected to throw Exception:" +  t);
        }
    }

    @Test(enabled=false)
    public void testGetAppConfigurationEntryStringForKafkaClient() {
        AppConfigurationEntry[] entries =
                Configuration.getConfiguration().getAppConfigurationEntry("KafkaClient");
        assertNotNull(entries);
        assertEquals(1, entries.length);
        String principal = (String) entries[0].getOptions().get("principal");
        assertNotNull(principal);
        String[] components = principal.split("[/@]");
        assertEquals(3, components.length);
        assertEquals(false, StringUtils.equalsIgnoreCase(components[1], "_HOST"));

    }

    @Test(enabled=false)
    public void testGetAppConfigurationEntryStringForMyClient() {
        AppConfigurationEntry[] entries =
                Configuration.getConfiguration().getAppConfigurationEntry("myClient");
        assertNotNull(entries);
        assertEquals(2, entries.length);
        String principal = (String) entries[0].getOptions().get("principal");
        assertNotNull(principal);
        String[] components = principal.split("[/@]");
        assertEquals(3, components.length);
        assertEquals(true, StringUtils.equalsIgnoreCase(components[1], "abcd"));

        principal = (String) entries[1].getOptions().get("principal");
        assertNotNull(principal);
        components = principal.split("[/@]");
        assertEquals(2, components.length);
    }

    @Test(enabled=false)
    public void testGetAppConfigurationEntryStringForUnknownClient() {
        AppConfigurationEntry[] entries =
                Configuration.getConfiguration().getAppConfigurationEntry("UnknownClient");
        assertNull(entries);
    }

}

