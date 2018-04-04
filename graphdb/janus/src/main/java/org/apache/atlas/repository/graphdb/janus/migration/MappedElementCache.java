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

package org.apache.atlas.repository.graphdb.janus.migration;

import org.apache.atlas.utils.LruCache;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.atlas.repository.Constants.EDGE_ID_IN_IMPORT_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_ID_IN_IMPORT_KEY;

public class MappedElementCache {
    private static final Logger LOG = LoggerFactory.getLogger(MappedElementCache.class);

    private final Map<Object, Vertex> lruVertexCache = new LruCache<>(500, 100000);
    private final Map<String, String> lruEdgeCache   = new LruCache<>(500, 100000);

    public Vertex getMappedVertex(Graph gr, Object key) {
        try {
            Vertex ret = lruVertexCache.get(key);

            if (ret == null) {
                synchronized (lruVertexCache) {
                    ret = lruVertexCache.get(key);

                    if(ret == null) {
                        ret = fetchVertex(gr, key);
                        lruVertexCache.put(key, ret);
                    }
                }
            }

            return ret;
        } catch (Exception ex) {
            LOG.error("getMappedVertex: {}", key, ex);
            return null;
        }
    }

    public String getMappedEdge(Graph gr, String key) {
        try {
            String ret = lruEdgeCache.get(key);

            if (ret == null) {
                synchronized (lruEdgeCache) {
                    ret = lruEdgeCache.get(key);

                    if (ret == null) {
                        Edge e = fetchEdge(gr, key);

                        ret = e.id().toString();

                        lruEdgeCache.put(key, ret);
                    }
                }
            }

            return ret;
        } catch (Exception ex) {
            LOG.error("getMappedEdge: {}", key, ex);
            return null;
        }
    }

    private Vertex fetchVertex(Graph gr, Object key) {
        try {
            return gr.traversal().V().has(VERTEX_ID_IN_IMPORT_KEY, key).next();
        } catch (Exception ex) {
            LOG.error("fetchVertex: fetchFromDB failed: {}", key);
            return null;
        }
    }

    private Edge fetchEdge(Graph gr, String key) {
        try {
            return gr.traversal().E().has(EDGE_ID_IN_IMPORT_KEY, key).next();
        } catch (Exception ex) {
            LOG.error("fetchEdge: fetchFromDB failed: {}", key);
            return null;
        }
    }

    public void clearVertexCache() {
        lruVertexCache.clear();
    }

    public void clearEdgeCache() {
        lruEdgeCache.clear();
    }

    public void clearAll() {
        clearVertexCache();
        clearEdgeCache();
    }
}
