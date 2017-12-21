/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.janusgraph.diskstorage.solr;

import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.diskstorage.configuration.Configuration;


import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Solr6Index implements IndexProvider {
    private final SolrIndex solrIndex;

    public Solr6Index(final Configuration config) throws BackendException {
        // Add Kerberos-enabled SolrHttpClientBuilder
        HttpClientUtil.setHttpClientBuilder(new Krb5HttpClientBuilder().getBuilder());

        solrIndex = new SolrIndex(config);
    }

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        solrIndex.register(store, key, information, tx);
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        solrIndex.mutate(mutations, informations, tx);
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        solrIndex.restore(documents, informations, tx);
    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        return solrIndex.query(query, informations, tx);
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        return solrIndex.query(query, informations, tx);
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        return solrIndex.totals(query, informations, tx);
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return solrIndex.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        solrIndex.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        solrIndex.clearStorage();
    }

    @Override
    public boolean exists() throws BackendException {
        return solrIndex.exists();
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        return solrIndex.supports(information, janusgraphPredicate);
    }

    @Override
    public boolean supports(KeyInformation information) {
        return solrIndex.supports(information);
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        return solrIndex.mapKey2Field(key, information);
    }

    @Override
    public IndexFeatures getFeatures() {
        return solrIndex.getFeatures();
    }
}