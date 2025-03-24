/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readOptionalStringProperty;

/**
 * It's a search request processor that rewrite one query to a different one.
 * Currently, we only rewrite `match` query to `neural_sparse` query.
 */
public class SemanticSearchRewriteProcessor extends AbstractProcessor implements SearchRequestProcessor {
    public static final String TYPE = "semantic_search_rewrite_processor";
    public static final String FIELD_MAP_FIELD = "field_map";
    public static final String DEFAULT_ANALYZER = "hf_model_tokenizer";
    public static final String ANALYZER_FIELD = "analyzer";

    private Map<String, Object> fieldMap;
    private String analyzer = DEFAULT_ANALYZER;

    protected SemanticSearchRewriteProcessor(
        String analyzer,
        Map<String, Object> fieldMap,
        String tag,
        String description,
        boolean ignoreFailure
    ) {
        super(tag, description, ignoreFailure);
        this.analyzer = StringUtils.isEmpty(analyzer) ? DEFAULT_ANALYZER : analyzer;
        this.fieldMap = fieldMap;
    }

    /**
     * Check if SearchRequest contains a MatchQueryBuilder then replace it with NeuralSparseQueryBuilder
     */
    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        if (request == null || request.source() == null || request.source().query() == null) {
            return request;
        }
        QueryBuilder queryBuilder = request.source().query();
        QueryBuilder newQueryBuilder = convertMatchQueryBuilderToNeuralSparseQueryBuilder(queryBuilder);
        request.source().query(newQueryBuilder);
        return request;
    }

    /**
     * Convert MatchQueryBuilder to NeuralSparseQueryBuilder
     * For compound QueryBuilder, we'll recursively convert the sub-QueryBuilder. And since they
     * don't support replace existing sub-QueryBuilder, we'll rebuild them.
     * @param queryBuilder
     * @return
     */
    private QueryBuilder convertMatchQueryBuilderToNeuralSparseQueryBuilder(QueryBuilder queryBuilder) {
        if (queryBuilder instanceof MatchQueryBuilder) {
            return convertMatchQueryBuilderToNeuralSparse((MatchQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof BoolQueryBuilder) {
            return copyBoolQueryBuilder((BoolQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof BoostingQueryBuilder) {
            return copyBoostingQueryBuilder((BoostingQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof ConstantScoreQueryBuilder) {
            return copyConstantScoreQueryBuilder((ConstantScoreQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof DisMaxQueryBuilder) {
            return copyDisMaxQueryBuilder((DisMaxQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof FunctionScoreQueryBuilder) {
            return copyFunctionScoreQueryBuilder((FunctionScoreQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof HybridQueryBuilder) {
            return copyHybridQueryBuilder((HybridQueryBuilder) queryBuilder);
        }
        return queryBuilder;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private QueryBuilder convertMatchQueryBuilderToNeuralSparse(MatchQueryBuilder matchQueryBuilder) {
        // if field name is not listed in fieldMap, then we don't convert it
        String fieldName = matchQueryBuilder.fieldName();
        if (!this.fieldMap.containsKey(fieldName)) {
            return matchQueryBuilder;
        }
        String mappedFieldName = this.fieldMap.get(fieldName).toString();
        return new NeuralSparseQueryBuilder().boost(matchQueryBuilder.boost())
            .fieldName(mappedFieldName)
            .queryText(matchQueryBuilder.value().toString())
            .analyzer(this.analyzer);
    }

    private BoolQueryBuilder copyBoolQueryBuilder(BoolQueryBuilder boolQueryBuilder) {
        BoolQueryBuilder newBoolQueryBuilder = new BoolQueryBuilder().queryName(boolQueryBuilder.queryName())
            .boost(boolQueryBuilder.boost())
            .adjustPureNegative(boolQueryBuilder.adjustPureNegative())
            .minimumShouldMatch(boolQueryBuilder.minimumShouldMatch());

        boolQueryBuilder.must().forEach(q -> newBoolQueryBuilder.must(convertMatchQueryBuilderToNeuralSparseQueryBuilder(q)));
        boolQueryBuilder.mustNot().forEach(q -> newBoolQueryBuilder.mustNot(convertMatchQueryBuilderToNeuralSparseQueryBuilder(q)));
        boolQueryBuilder.filter().forEach(q -> newBoolQueryBuilder.filter(convertMatchQueryBuilderToNeuralSparseQueryBuilder(q)));
        boolQueryBuilder.should().forEach(q -> newBoolQueryBuilder.should(convertMatchQueryBuilderToNeuralSparseQueryBuilder(q)));

        return newBoolQueryBuilder;
    }

    private BoostingQueryBuilder copyBoostingQueryBuilder(BoostingQueryBuilder boostingQueryBuilder) {
        QueryBuilder newPositiveQueryBuilder = convertMatchQueryBuilderToNeuralSparseQueryBuilder(boostingQueryBuilder.positiveQuery());
        QueryBuilder newNegativeQueryBuilder = convertMatchQueryBuilderToNeuralSparseQueryBuilder(boostingQueryBuilder.negativeQuery());
        return new BoostingQueryBuilder(newPositiveQueryBuilder, newNegativeQueryBuilder).queryName(boostingQueryBuilder.queryName())
            .boost(boostingQueryBuilder.boost())
            .negativeBoost(boostingQueryBuilder.negativeBoost());
    }

    private ConstantScoreQueryBuilder copyConstantScoreQueryBuilder(ConstantScoreQueryBuilder constantScoreQueryBuilder) {
        QueryBuilder newInnerQueryBuilder = convertMatchQueryBuilderToNeuralSparseQueryBuilder(constantScoreQueryBuilder.innerQuery());
        return new ConstantScoreQueryBuilder(newInnerQueryBuilder).queryName(constantScoreQueryBuilder.queryName())
            .boost(constantScoreQueryBuilder.boost());
    }

    private DisMaxQueryBuilder copyDisMaxQueryBuilder(DisMaxQueryBuilder disMaxQueryBuilder) {
        DisMaxQueryBuilder newDisMaxQueryBuilder = new DisMaxQueryBuilder().queryName(disMaxQueryBuilder.queryName())
            .tieBreaker(disMaxQueryBuilder.tieBreaker())
            .boost(disMaxQueryBuilder.boost());

        disMaxQueryBuilder.innerQueries().forEach(q -> newDisMaxQueryBuilder.add(convertMatchQueryBuilderToNeuralSparseQueryBuilder(q)));

        return newDisMaxQueryBuilder;
    }

    private FunctionScoreQueryBuilder copyFunctionScoreQueryBuilder(FunctionScoreQueryBuilder functionScoreQueryBuilder) {
        QueryBuilder newQueryBuilder = convertMatchQueryBuilderToNeuralSparseQueryBuilder(functionScoreQueryBuilder.query());
        return new FunctionScoreQueryBuilder(newQueryBuilder, functionScoreQueryBuilder.filterFunctionBuilders()).queryName(
            functionScoreQueryBuilder.queryName()
        )
            .boost(functionScoreQueryBuilder.boost())
            .boostMode(functionScoreQueryBuilder.boostMode())
            .maxBoost(functionScoreQueryBuilder.maxBoost())
            .scoreMode(functionScoreQueryBuilder.scoreMode())
            .setMinScore(functionScoreQueryBuilder.getMinScore());
    }

    private HybridQueryBuilder copyHybridQueryBuilder(HybridQueryBuilder hybridQueryBuilder) {
        HybridQueryBuilder newHybridQueryBuilder = new HybridQueryBuilder().queryName(hybridQueryBuilder.queryName())
            .fieldName(hybridQueryBuilder.fieldName())
            .boost(hybridQueryBuilder.boost());
        hybridQueryBuilder.queries().forEach(q -> newHybridQueryBuilder.add(convertMatchQueryBuilderToNeuralSparseQueryBuilder(q)));

        return newHybridQueryBuilder;
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {

        @Override
        public SearchRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            Map<String, Object> fieldMap = readMap(TYPE, tag, config, FIELD_MAP_FIELD);
            String analyzer = readOptionalStringProperty(TYPE, tag, config, ANALYZER_FIELD);
            return new SemanticSearchRewriteProcessor(analyzer, fieldMap, tag, description, ignoreFailure);
        }
    }
}
