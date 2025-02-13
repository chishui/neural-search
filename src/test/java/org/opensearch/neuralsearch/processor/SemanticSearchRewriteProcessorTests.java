/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SemanticSearchRewriteProcessorTests extends OpenSearchTestCase {
    private static final String TAG_FIELD = "tag";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String MODEL_VALUE = "model_value";
    private static final String MAPPING_FIELD = "field_map";
    private static final String MAPPED_FIELD = "text";
    private static final String EMBEDDING_FIELD = "text_embedding";
    private static final String QUERY = "hello";

    public void testProcessRequest_NullRequest() throws Exception {
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        assertNull(processor.processRequest(null));
    }

    public void testProcessRequest_EmptySource() throws Exception {
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder());
        assertEquals(searchRequest, processor.processRequest(searchRequest));
    }

    public void testProcessRequest_EmptyQuery() throws Exception {
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(null));
        assertEquals(searchRequest, processor.processRequest(searchRequest));
    }

    public void testProcessRequest_SourceQueryIsMatchQuery() throws Exception {
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder(MAPPED_FIELD, QUERY)));
        SearchRequest returnedRequest = processor.processRequest(searchRequest);
        assertTrue(returnedRequest.source().query() instanceof NeuralSparseQueryBuilder);
        NeuralSparseQueryBuilder query = (NeuralSparseQueryBuilder) returnedRequest.source().query();
        assertEquals(MODEL_VALUE, query.modelId());
        assertEquals(EMBEDDING_FIELD, query.fieldName());
        assertEquals(QUERY, query.queryText());
    }

    public void testProcessRequest_SourceQueryIsBoolQuery() throws Exception {
        // prepare
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new MatchQueryBuilder(MAPPED_FIELD, QUERY));
        boolQueryBuilder.must(new TermQueryBuilder(MAPPED_FIELD, 2));
        boolQueryBuilder.mustNot(new TermQueryBuilder(MAPPED_FIELD, 2));
        boolQueryBuilder.filter(new TermQueryBuilder(MAPPED_FIELD, 2));
        boolQueryBuilder.should(new MatchQueryBuilder(MAPPED_FIELD, QUERY));
        searchRequest.source(new SearchSourceBuilder().query(boolQueryBuilder));
        // run
        SearchRequest returnedRequest = processor.processRequest(searchRequest);
        // validate
        assertTrue(returnedRequest.source().query() instanceof BoolQueryBuilder);
        BoolQueryBuilder query = (BoolQueryBuilder) returnedRequest.source().query();
        assertEquals(2, query.must().size());
        assertTrue(query.must().get(0) instanceof NeuralSparseQueryBuilder);
        assertTrue(query.must().get(1) instanceof TermQueryBuilder);
        validateNeuralSparseQueryBuilder((NeuralSparseQueryBuilder) query.must().get(0));
        assertTrue(query.mustNot().get(0) instanceof TermQueryBuilder);
        assertTrue(query.filter().get(0) instanceof TermQueryBuilder);
        assertTrue(query.should().get(0) instanceof NeuralSparseQueryBuilder);
        validateNeuralSparseQueryBuilder((NeuralSparseQueryBuilder) query.should().get(0));
    }

    public void testProcessRequest_SourceQueryIsBoostingQueryBuilder() throws Exception {
        // prepare
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        BoostingQueryBuilder boostingQueryBuilder = new BoostingQueryBuilder(
            new MatchQueryBuilder(MAPPED_FIELD, QUERY),
            new TermQueryBuilder(MAPPED_FIELD, 2)
        ).negativeBoost(1.0f);
        searchRequest.source(new SearchSourceBuilder().query(boostingQueryBuilder));
        // run
        SearchRequest returnedRequest = processor.processRequest(searchRequest);
        // validate
        assertTrue(returnedRequest.source().query() instanceof BoostingQueryBuilder);
        BoostingQueryBuilder query = (BoostingQueryBuilder) returnedRequest.source().query();
        assertTrue(query.positiveQuery() instanceof NeuralSparseQueryBuilder);
        assertTrue(query.negativeQuery() instanceof TermQueryBuilder);
        validateNeuralSparseQueryBuilder((NeuralSparseQueryBuilder) query.positiveQuery());
    }

    public void testProcessRequest_SourceQueryIsConstantScoreQueryBuilder() throws Exception {
        // prepare
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(new MatchQueryBuilder(MAPPED_FIELD, QUERY));
        searchRequest.source(new SearchSourceBuilder().query(constantScoreQueryBuilder));
        // run
        SearchRequest returnedRequest = processor.processRequest(searchRequest);
        // validate
        ConstantScoreQueryBuilder query = (ConstantScoreQueryBuilder) returnedRequest.source().query();
        assertTrue(query.innerQuery() instanceof NeuralSparseQueryBuilder);
        validateNeuralSparseQueryBuilder((NeuralSparseQueryBuilder) query.innerQuery());
    }

    public void testProcessRequest_SourceQueryIsNeuralSparseQueryBuilder() throws Exception {
        // prepare
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName(EMBEDDING_FIELD);
        neuralSparseQueryBuilder.queryText(QUERY);
        neuralSparseQueryBuilder.modelId(MODEL_VALUE);
        searchRequest.source(new SearchSourceBuilder().query(neuralSparseQueryBuilder));
        // run
        SearchRequest returnedRequest = processor.processRequest(searchRequest);
        // validate
        assertEquals(neuralSparseQueryBuilder, returnedRequest.source().query());
    }

    public void testProcessRequest_SourceQueryIsDisMaxQueryBuilder() throws Exception {
        // prepare
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        DisMaxQueryBuilder disMaxQueryBuilder = new DisMaxQueryBuilder();
        disMaxQueryBuilder.add(new MatchQueryBuilder(MAPPED_FIELD, QUERY));
        disMaxQueryBuilder.add(new TermQueryBuilder(MAPPED_FIELD, 2));
        searchRequest.source(new SearchSourceBuilder().query(disMaxQueryBuilder));
        // run
        SearchRequest returnedRequest = processor.processRequest(searchRequest);
        // validate
        assertTrue(returnedRequest.source().query() instanceof DisMaxQueryBuilder);
        DisMaxQueryBuilder query = (DisMaxQueryBuilder) returnedRequest.source().query();
        assertEquals(2, query.innerQueries().size());
        assertTrue(query.innerQueries().get(0) instanceof NeuralSparseQueryBuilder);
        validateNeuralSparseQueryBuilder((NeuralSparseQueryBuilder) query.innerQueries().get(0));
        assertTrue(query.innerQueries().get(1) instanceof TermQueryBuilder);
    }

    public void testProcessRequest_SourceQueryIsFunctionScoreQueryBuilder() throws Exception {
        // prepare
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(new MatchQueryBuilder(MAPPED_FIELD, QUERY));
        functionScoreQueryBuilder.boostMode(CombineFunction.AVG)
            .setMinScore(1.0f)
            .maxBoost(1.0f)
            .scoreMode(FunctionScoreQuery.ScoreMode.AVG);
        searchRequest.source(new SearchSourceBuilder().query(functionScoreQueryBuilder));
        // run
        SearchRequest returnedRequest = processor.processRequest(searchRequest);
        // validate
        assertTrue(returnedRequest.source().query() instanceof FunctionScoreQueryBuilder);
        FunctionScoreQueryBuilder query = (FunctionScoreQueryBuilder) returnedRequest.source().query();
        assertTrue(query.query() instanceof NeuralSparseQueryBuilder);
        validateNeuralSparseQueryBuilder((NeuralSparseQueryBuilder) query.query());
    }

    public void testProcessRequest_SourceQueryIsHybridQueryBuilder() throws Exception {
        // prepare
        SemanticSearchRewriteProcessor processor = createTestProcessor();
        SearchRequest searchRequest = new SearchRequest();
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(new MatchQueryBuilder(MAPPED_FIELD, QUERY));
        hybridQueryBuilder.add(new TermQueryBuilder(MAPPED_FIELD, 2));
        searchRequest.source(new SearchSourceBuilder().query(hybridQueryBuilder));
        // run
        SearchRequest returnedRequest = processor.processRequest(searchRequest);
        // validate
        assertTrue(returnedRequest.source().query() instanceof HybridQueryBuilder);
        HybridQueryBuilder query = (HybridQueryBuilder) returnedRequest.source().query();
        assertEquals(2, query.queries().size());
        assertTrue(query.queries().get(0) instanceof NeuralSparseQueryBuilder);
        validateNeuralSparseQueryBuilder((NeuralSparseQueryBuilder) query.queries().get(0));
        assertTrue(query.queries().get(1) instanceof TermQueryBuilder);
    }

    private void validateNeuralSparseQueryBuilder(NeuralSparseQueryBuilder neuralQuery) {
        assertEquals(MODEL_VALUE, neuralQuery.modelId());
        assertEquals(EMBEDDING_FIELD, neuralQuery.fieldName());
        assertEquals(QUERY, neuralQuery.queryText());
    }

    private SemanticSearchRewriteProcessor createTestProcessor() throws Exception {
        SemanticSearchRewriteProcessor.Factory factory = new SemanticSearchRewriteProcessor.Factory();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TAG_FIELD, TAG_FIELD);
        configMap.put(DESCRIPTION_FIELD, DESCRIPTION_FIELD);
        configMap.put(MODEL_ID_FIELD, MODEL_VALUE);
        Map<String, Object> mappingMap = new HashMap<>();
        mappingMap.put(MAPPED_FIELD, EMBEDDING_FIELD);
        configMap.put(MAPPING_FIELD, mappingMap);
        return (SemanticSearchRewriteProcessor) factory.create(Collections.emptyMap(), null, null, false, configMap, null);
    }
}
