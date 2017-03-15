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

package org.apache.atlas.discovery.graph;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.TitanIndexQuery.Result;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.discovery.DiscoveryException;
import org.apache.atlas.discovery.DiscoveryService;
import org.apache.atlas.query.Expressions;
import org.apache.atlas.query.GremlinEvaluator;
import org.apache.atlas.query.GremlinQuery;
import org.apache.atlas.query.GremlinQueryResult;
import org.apache.atlas.query.GremlinTranslator;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.query.QueryParser;
import org.apache.atlas.query.QueryProcessor;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.parsing.combinator.Parsers;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Graph backed implementation of Search.
 */
@Singleton
public class GraphBackedDiscoveryService implements DiscoveryService {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedDiscoveryService.class);

    private final TitanGraph titanGraph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;
    private final TypeSystem typeSystem = TypeSystem.getInstance();

    public final static String SCORE = "score";

    private final static String BASIC_SEARCH_TYPE_FILTER           = ".has('__typeName', T.in, typeNames)";
    private final static String BASIC_SEARCH_CLASSIFICATION_FILTER = ".has('__traitNames', T.in, traitNames)";
    private final static String TO_RANGE_LIST                      = " [startIdx..<endIdx].toList()";
    private  final static String TAXONOMY_TERM_TYPE                = "TaxonomyTerm";
    private final static String TERM_LABEL                         = "terms";
    private final static String ID_LABEL                           = "guid";

    @Inject
    GraphBackedDiscoveryService(GraphProvider<TitanGraph> graphProvider, MetadataRepository metadataRepository)
    throws DiscoveryException {
        this.titanGraph = graphProvider.get();
        this.graphPersistenceStrategy = new DefaultGraphPersistenceStrategy(metadataRepository);
    }

    //Refer http://s3.thinkaurelius.com/docs/titan/0.5.4/index-backends.html for indexed query
    //http://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query
    // .html#query-string-syntax for query syntax
    @Override
    @GraphTransaction
    public String searchByFullText(String query, QueryParams queryParams) throws DiscoveryException {
        String graphQuery = String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, query);
        LOG.debug("Full text query: {}", graphQuery);
        Iterator<TitanIndexQuery.Result<Vertex>> results =
                titanGraph.indexQuery(Constants.FULLTEXT_INDEX, graphQuery).vertices().iterator();
        JSONArray response = new JSONArray();

        int index = 0;
        while (results.hasNext() && index < queryParams.offset()) {
            results.next();
            index++;
        }

        while (results.hasNext() && response.length() < queryParams.limit()) {
            TitanIndexQuery.Result<Vertex> result = results.next();
            Vertex vertex = result.getElement();

            JSONObject row = new JSONObject();
            String guid = GraphHelper.getIdFromVertex(vertex);
            if (guid != null) { //Filter non-class entities
                try {
                    row.put("guid", guid);
                    row.put(AtlasClient.TYPENAME, GraphHelper.getTypeName(vertex));
                    row.put(SCORE, result.getScore());
                } catch (JSONException e) {
                    LOG.error("Unable to create response", e);
                    throw new DiscoveryException("Unable to create response");
                }

                response.put(row);
            }
        }
        return response.toString();
    }

    @Override
    public String basicSearchByFullText(String query, String typeName, String trait, QueryParams params) throws DiscoveryException {
        JSONArray   response   = new JSONArray();
        Set<String> typeNames  = null;
        Set<String> traitNames = null;

        if (StringUtils.isNotEmpty(typeName)) {
            try {
                ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
                typeNames           = classType.getTypeAndAllSubTypes();

            } catch (AtlasException e) {
                LOG.error("Unable to find type : {0}", typeName);
                throw new DiscoveryException("Unable to find type : " + typeName);
            }
        }

        if (StringUtils.isNotEmpty(trait)) {
            try {
                TraitType traitType = typeSystem.getDataType(TraitType.class, trait);
                traitNames          = traitType.getTypeAndAllSubTypes();

            } catch (AtlasException e) {
                LOG.error("Unable to find trait : {0}", trait);
                throw new DiscoveryException("Unable to find trait : " + trait);
            }
        }

        if (StringUtils.isNotEmpty(query)) {
            final String                   idxQuery   = String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, query);
            final Iterator<Result<Vertex>> qryResults = titanGraph.indexQuery(Constants.FULLTEXT_INDEX, idxQuery).vertices().iterator();
            final int                      startIdx   = params.offset();
            final int                      resultSize = params.limit();

            int resultIdx = 0;

            while (qryResults.hasNext()) {
                Result<Vertex> qryResult      = qryResults.next();
                Vertex         vertex         = qryResult.getElement();
                String         vertexTypeName = GraphHelper.getTypeName(vertex);
                String         guid           = GraphHelper.getIdFromVertex(vertex);

                // skip non-entity vertices
                if (StringUtils.isEmpty(vertexTypeName) || StringUtils.isEmpty(guid)) {
                    continue;
                }

                if (typeNames != null && !typeNames.contains(vertexTypeName)) {
                    continue;
                }

                if (traitNames != null) {
                    List<String> traits = GraphHelper.getTraitNames(vertex);

                    if (CollectionUtils.isEmpty(traitNames) || !CollectionUtils.containsAny(traits, traitNames)) {
                        continue;
                    }
                }

                resultIdx++;

                if (resultIdx <= startIdx) {
                    continue;
                }

                JSONObject result = constructBasicSearchResult(vertex);

                response.put(result);

                if (response.length() == resultSize) {
                    break;
                }
            }
        }

        else {
            ScriptEngine scriptEngine = getGremlinScriptEngine();
            Bindings     bindings     = scriptEngine.createBindings();
            String       basicQuery   = "g.V()";

            bindings.put("g", titanGraph);

            if (typeNames != null) {
                bindings.put("typeNames", typeNames);

                basicQuery += BASIC_SEARCH_TYPE_FILTER;
            }

            if (traitNames != null) {
                bindings.put("traitNames", traitNames);

                basicQuery += BASIC_SEARCH_CLASSIFICATION_FILTER;
            }

            // add range and offset bindings
            bindings.put("startIdx", params.offset());
            bindings.put("endIdx", params.offset() + params.limit());

            basicQuery += TO_RANGE_LIST;

            try {
                Object result = scriptEngine.eval(basicQuery, bindings);

                if (result instanceof List && CollectionUtils.isNotEmpty((List) result)) {
                    List   queryResult  = (List) result;
                    Object firstElement = queryResult.get(0);

                    if (firstElement instanceof Vertex) {
                        for (Object element : queryResult) {
                            if (element instanceof Vertex) {
                                result = constructBasicSearchResult((Vertex) element);

                                response.put(result);
                            } else {
                                LOG.warn("searchUsingBasicQuery({}): expected Vertex; found unexpected entry in result {}", basicQuery, element);
                            }
                        }
                    }
                }
            } catch (ScriptException e) {
                throw new DiscoveryException("Basic search query failed");
            }
        }

        return response.toString();
    }

    @Override
    @GraphTransaction
    public String searchByDSL(String dslQuery, QueryParams queryParams) throws DiscoveryException {
        GremlinQueryResult queryResult = evaluate(dslQuery, queryParams);
        return queryResult.toJson();
    }

    public GremlinQueryResult evaluate(String dslQuery, QueryParams queryParams) throws DiscoveryException {
        LOG.debug("Executing dsl query={}", dslQuery);
        try {
            Either<Parsers.NoSuccess, Expressions.Expression> either = QueryParser.apply(dslQuery, queryParams);
            if (either.isRight()) {
                Expressions.Expression expression = either.right().get();
                return evaluate(dslQuery, expression);
            } else {
                throw new DiscoveryException("Invalid expression : " + dslQuery + ". " + either.left());
            }
        } catch (Exception e) { // unable to catch ExpressionException
            throw new DiscoveryException("Invalid expression : " + dslQuery, e);
        }
    }

    private GremlinQueryResult evaluate(String dslQuery, Expressions.Expression expression) {
        Expressions.Expression validatedExpression = QueryProcessor.validate(expression);

        //If the final limit is 0, don't launch the query, return with 0 rows
        if (validatedExpression instanceof Expressions.LimitExpression
                && ((Integer)((Expressions.LimitExpression) validatedExpression).limit().rawValue()) == 0) {
            return new GremlinQueryResult(dslQuery, validatedExpression.dataType(), Collections.emptyList());
        }

        GremlinQuery gremlinQuery = new GremlinTranslator(validatedExpression, graphPersistenceStrategy).translate();
        LOG.debug("Query = {}", validatedExpression);
        LOG.debug("Expression Tree = {}", validatedExpression.treeString());
        LOG.debug("Gremlin Query = {}", gremlinQuery.queryStr());
        return new GremlinEvaluator(gremlinQuery, graphPersistenceStrategy, titanGraph).evaluate();
    }

    /**
     * Assumes the User is familiar with the persistence structure of the Repository.
     * The given query is run uninterpreted against the underlying Graph Store.
     * The results are returned as a List of Rows. each row is a Map of Key,Value pairs.
     *
     * @param gremlinQuery query in gremlin dsl format
     * @return List of Maps
     * @throws org.apache.atlas.discovery.DiscoveryException
     */
    @Override
    @GraphTransaction
    public List<Map<String, String>> searchByGremlin(String gremlinQuery) throws DiscoveryException {
        LOG.debug("Executing gremlin query={}", gremlinQuery);
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("gremlin-groovy");

        if(engine == null) {
            throw new DiscoveryException("gremlin-groovy: engine not found");
        }

        Bindings bindings = engine.createBindings();
        bindings.put("g", titanGraph);

        try {
            Object o = engine.eval(gremlinQuery, bindings);
            return extractResult(o);
        } catch (ScriptException se) {
            throw new DiscoveryException(se);
        }
    }

    private List<Map<String, String>> extractResult(final Object o) throws DiscoveryException {
        List<Map<String, String>> result = new ArrayList<>();
        if (o instanceof List) {
            List l = (List) o;
            for (Object r : l) {

                Map<String, String> oRow = new HashMap<>();
                if (r instanceof Map) {
                    @SuppressWarnings("unchecked") Map<Object, Object> iRow = (Map) r;
                    for (Map.Entry e : iRow.entrySet()) {
                        Object k = e.getKey();
                        Object v = e.getValue();
                        oRow.put(k.toString(), v.toString());
                    }
                } else if (r instanceof TitanVertex) {
                    TitanVertex vertex = (TitanVertex) r;
                    oRow.put("id", vertex.getId().toString());
                    Iterable<TitanProperty> ps = vertex.getProperties();
                    for (TitanProperty tP : ps) {
                        String pName = tP.getPropertyKey().getName();
                        Object pValue = vertex.getProperty(pName);
                        if (pValue != null) {
                            oRow.put(pName, pValue.toString());
                        }
                    }

                } else if (r instanceof String) {
                    oRow.put("", r.toString());
                } else if (r instanceof TitanEdge) {
                    TitanEdge edge = (TitanEdge) r;
                    oRow.put("id", edge.getId().toString());
                    oRow.put("label", edge.getLabel());
                    oRow.put("inVertex", edge.getVertex(Direction.IN).getId().toString());
                    oRow.put("outVertex", edge.getVertex(Direction.OUT).getId().toString());
                    Set<String> propertyKeys = edge.getPropertyKeys();
                    for (String propertyKey : propertyKeys) {
                        oRow.put(propertyKey, edge.getProperty(propertyKey).toString());
                    }
                } else {
                    throw new DiscoveryException(String.format("Cannot process result %s", o.toString()));
                }

                result.add(oRow);
            }
        } else {
            result.add(new HashMap<String, String>() {{
                put("result", o.toString());
            }});
        }
        return result;
    }

    private ScriptEngine getGremlinScriptEngine() throws DiscoveryException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine        engine  = manager.getEngineByName("gremlin-groovy");

        if(engine == null) {
            throw new DiscoveryException("gremlin-groovy: engine not found");
        }

        return engine;
    }

    private JSONObject constructBasicSearchResult(Vertex vertex) {
        JSONObject   ret    = new JSONObject();
        List<String> traits = new ArrayList<>();
        List<String> terms  = new ArrayList<>();

        List<String> traitsAndTerms = GraphHelper.getTraitNames(vertex);

        if (CollectionUtils.isNotEmpty(traitsAndTerms)) {
            for (String t : traitsAndTerms) {
                try {
                    TraitType   traitType  = typeSystem.getDataType(TraitType.class, t);
                    Set<String> superTypes = traitType.getAllSuperTypeNames();

                    if (CollectionUtils.isNotEmpty(superTypes) && superTypes.contains(TAXONOMY_TERM_TYPE)) {
                        terms.add(t);
                    } else {
                        traits.add(t);
                    }
                } catch (AtlasException e) {
                    LOG.warn("Unable to find trait : {0}", t);
                }
            }
        }

        String name = GraphHelper.getSingleValuedProperty(vertex, Constants.NAME_PROPERTY_KEY);

        if (StringUtils.isEmpty(name)) {
            name = GraphHelper.getSingleValuedProperty(vertex, Constants.QUALIFIED_NAME_PROPERTY_KEY);
        }

        try {
            ret.put(ID_LABEL, GraphHelper.getIdFromVertex(vertex));
            ret.put(AtlasClient.TYPENAME, GraphHelper.getTypeName(vertex));
            ret.put(AtlasClient.NAME, name);
            ret.put(AtlasClient.DESCRIPTION, GraphHelper.getSingleValuedProperty(vertex, Constants.DESCRIPTION_PROPERTY_KEY));
            ret.put(AtlasClient.OWNER, GraphHelper.getSingleValuedProperty(vertex, Constants.OWNER_PROPERTY_KEY));
            ret.put(AtlasClient.URI_TRAITS, traits);
            ret.put(TERM_LABEL, terms);

        } catch (JSONException e) {
            LOG.warn("Unable to construct basic search result!", e);
        }

        return ret;
    }
}
