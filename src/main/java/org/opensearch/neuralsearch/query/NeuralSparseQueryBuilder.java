/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
<<<<<<< HEAD
import org.opensearch.client.Client;
=======
import org.opensearch.neuralsearch.processor.util.DocumentClusterManager;
import org.opensearch.neuralsearch.processor.util.DocumentClusterUtils;
import org.opensearch.transport.client.Client;
>>>>>>> 0c0d49a8 (Feat: Introduce document cluster manager (#2))
import org.opensearch.common.SetOnce;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.TokenWeightUtil;

import com.google.common.annotations.VisibleForTesting;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import static org.opensearch.neuralsearch.processor.NeuralSparseTwoPhaseProcessor.splitQueryTokensByRatioedMaxScoreAsThreshold;

import static java.lang.Math.max;
import static org.opensearch.neuralsearch.processor.util.DocumentClusterManager.SKETCH_SIZE;

/**
 * SparseEncodingQueryBuilder is responsible for handling "neural_sparse" query types. It uses an ML NEURAL_SPARSE model
 * or SPARSE_TOKENIZE model to produce a Map with String keys and Float values for input text. Then it will be transformed
 * to Lucene FeatureQuery wrapped by Lucene BooleanQuery.
 */

@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class NeuralSparseQueryBuilder extends AbstractQueryBuilder<NeuralSparseQueryBuilder> implements ModelInferenceQueryBuilder {
    public static final String NAME = "neural_sparse";
    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    @VisibleForTesting
    static final ParseField QUERY_TOKENS_FIELD = new ParseField("query_tokens");
    @VisibleForTesting
    static final ParseField MODEL_ID_FIELD = new ParseField("model_id");
    static final ParseField SEARCH_CLUSTER_FIELD = new ParseField("search_cluster");
    static final ParseField DOCUMENT_RATIO_FIELD = new ParseField("document_ratio");
    // We use max_token_score field to help WAND scorer prune query clause in lucene 9.7. But in lucene 9.8 the inner
    // logics change, this field is not needed any more.
    @VisibleForTesting
    @Deprecated
    static final ParseField MAX_TOKEN_SCORE_FIELD = new ParseField("max_token_score").withAllDeprecated();
    private static MLCommonsClientAccessor ML_CLIENT;
    private String fieldName;
    private String queryText;
    private String modelId;
    private Float maxTokenScore;
    private Boolean searchCluster;
    private Float documentRatio;
    private Supplier<Map<String, Float>> queryTokensSupplier;
    // A field that for neural_sparse_two_phase_processor, if twoPhaseSharedQueryToken is not null,
    // it means it's origin NeuralSparseQueryBuilder and should split the low score tokens form itself then put it into
    // twoPhaseSharedQueryToken.
    private Map<String, Float> twoPhaseSharedQueryToken;
    // A parameter with a default value 0F,
    // 1. If the query request are using neural_sparse_two_phase_processor and be collected,
    // It's value will be the ratio of processor.
    // 2. If it's the sub query only build for two-phase, the value will be set to -1 * ratio of processor.
    // Then in the DoToQuery, we can use this to determine which type are this queryBuilder.
    private float twoPhasePruneRatio = 0F;

    private static final Version MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID = Version.V_2_13_0;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralSparseQueryBuilder.ML_CLIENT = mlClient;
    }

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public NeuralSparseQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.queryText = in.readString();
        if (isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
            this.modelId = in.readOptionalString();
        } else {
            this.modelId = in.readString();
        }
        this.maxTokenScore = in.readOptionalFloat();
        if (in.readBoolean()) {
            Map<String, Float> queryTokens = in.readMap(StreamInput::readString, StreamInput::readFloat);
            this.queryTokensSupplier = () -> queryTokens;
        }
        // to be backward compatible with previous version, we need to use writeString/readString API instead of optionalString API
        // after supporting query by tokens, queryText and modelId can be null. here we write an empty String instead
        if (StringUtils.EMPTY.equals(this.queryText)) {
            this.queryText = null;
        }
        if (StringUtils.EMPTY.equals(this.modelId)) {
            this.modelId = null;
        }
        this.searchCluster = in.readOptionalBoolean();
        this.documentRatio = in.readOptionalFloat();
    }

    /**
     * Copy this QueryBuilder for two phase rescorer, set the copy one's twoPhasePruneRatio to -1.
     * @param ratio the parameter of the NeuralSparseTwoPhaseProcessor, control how to split the queryTokens to two phase.
     * @return A copy NeuralSparseQueryBuilder for twoPhase, it will be added to the rescorer.
     */
    public NeuralSparseQueryBuilder getCopyNeuralSparseQueryBuilderForTwoPhase(float ratio) {
        this.twoPhasePruneRatio(ratio);
        NeuralSparseQueryBuilder copy = new NeuralSparseQueryBuilder().fieldName(this.fieldName)
            .queryName(this.queryName)
            .queryText(this.queryText)
            .modelId(this.modelId)
            .maxTokenScore(this.maxTokenScore)
            .twoPhasePruneRatio(-1f * pruneRatio)
            .searchCluster(this.searchCluster)
            .documentRatio(this.documentRatio);
        if (Objects.nonNull(this.queryTokensSupplier)) {
            Map<String, Float> tokens = queryTokensSupplier.get();
            // Splitting tokens based on a threshold value: tokens greater than the threshold are stored in v1,
            // while those less than or equal to the threshold are stored in v2.
            Tuple<Map<String, Float>, Map<String, Float>> splitTokens = splitQueryTokensByRatioedMaxScoreAsThreshold(tokens, ratio);
            this.queryTokensSupplier(() -> splitTokens.v1());
            copy.queryTokensSupplier(() -> splitTokens.v2());
        } else {
            this.twoPhaseSharedQueryToken = new HashMap<>();
            copy.queryTokensSupplier(() -> this.twoPhaseSharedQueryToken);
        }
        return copy;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        // to be backward compatible with previous version, we need to use writeString/readString API instead of optionalString API
        // after supporting query by tokens, queryText and modelId can be null. here we write an empty String instead
        out.writeString(StringUtils.defaultString(this.queryText, StringUtils.EMPTY));
        if (isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
            out.writeOptionalString(this.modelId);
        } else {
            out.writeString(StringUtils.defaultString(this.modelId, StringUtils.EMPTY));
        }
        out.writeOptionalFloat(maxTokenScore);
        if (!Objects.isNull(this.queryTokensSupplier) && !Objects.isNull(this.queryTokensSupplier.get())) {
            out.writeBoolean(true);
            out.writeMap(this.queryTokensSupplier.get(), StreamOutput::writeString, StreamOutput::writeFloat);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalBoolean(this.searchCluster);
        out.writeOptionalFloat(this.documentRatio);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        xContentBuilder.startObject(fieldName);
        if (Objects.nonNull(queryText)) {
            xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        }
        if (Objects.nonNull(modelId)) {
            xContentBuilder.field(MODEL_ID_FIELD.getPreferredName(), modelId);
        }
        if (Objects.nonNull(maxTokenScore)) {
            xContentBuilder.field(MAX_TOKEN_SCORE_FIELD.getPreferredName(), maxTokenScore);
        }
        if (Objects.nonNull(queryTokensSupplier) && Objects.nonNull(queryTokensSupplier.get())) {
            xContentBuilder.field(QUERY_TOKENS_FIELD.getPreferredName(), queryTokensSupplier.get());
        }
        xContentBuilder.field(SEARCH_CLUSTER_FIELD.getPreferredName(), searchCluster);
        if (Objects.nonNull(documentRatio)) {
            xContentBuilder.field(DOCUMENT_RATIO_FIELD.getPreferredName(), documentRatio);
        }
        printBoostAndQueryName(xContentBuilder);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    /**
     * The expected parsing form looks like:
     *  "SAMPLE_FIELD": {
     *    "query_text": "string",
     *    "model_id": "string",
     *    "token_score_upper_bound": float (optional)
     *  }
     *
     *  or
     *  "SAMPLE_FIELD": {
     *      "query_tokens": {
     *          "token_a": float,
     *          "token_b": float,
     *          ...
     *       }
     *  }
     *
     *
     * @param parser XContentParser
     * @return NeuralQueryBuilder
     * @throws IOException can be thrown by parser
     */
    public static NeuralSparseQueryBuilder fromXContent(XContentParser parser) throws IOException {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder();
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "First token of " + NAME + "query must be START_OBJECT");
        }
        parser.nextToken();
        sparseEncodingQueryBuilder.fieldName(parser.currentName());
        parser.nextToken();
        parseQueryParams(parser, sparseEncodingQueryBuilder);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(
                    Locale.ROOT,
                    "[%s] query doesn't support multiple fields, found [%s] and [%s]",
                    NAME,
                    sparseEncodingQueryBuilder.fieldName(),
                    parser.currentName()
                )
            );
        }

        requireValue(sparseEncodingQueryBuilder.fieldName(), "Field name must be provided for " + NAME + " query");
        if (Objects.isNull(sparseEncodingQueryBuilder.queryTokensSupplier())) {
            requireValue(
                sparseEncodingQueryBuilder.queryText(),
                String.format(
                    Locale.ROOT,
                    "either %s field or %s field must be provided for [%s] query",
                    QUERY_TEXT_FIELD.getPreferredName(),
                    QUERY_TOKENS_FIELD.getPreferredName(),
                    NAME
                )
            );
            if (!isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
                requireValue(
                    sparseEncodingQueryBuilder.modelId(),
                    String.format(
                        Locale.ROOT,
                        "using %s, %s field must be provided for [%s] query",
                        QUERY_TEXT_FIELD.getPreferredName(),
                        MODEL_ID_FIELD.getPreferredName(),
                        NAME
                    )
                );
            }
        }

        if (StringUtils.EMPTY.equals(sparseEncodingQueryBuilder.queryText())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "%s field can not be empty", QUERY_TEXT_FIELD.getPreferredName())
            );
        }
        if (StringUtils.EMPTY.equals(sparseEncodingQueryBuilder.modelId())) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s field can not be empty", MODEL_ID_FIELD.getPreferredName()));
        }

        return sparseEncodingQueryBuilder;
    }

    private static void parseQueryParams(XContentParser parser, NeuralSparseQueryBuilder sparseEncodingQueryBuilder) throws IOException {
        XContentParser.Token token;
        String currentFieldName = "";
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.queryName(parser.text());
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.boost(parser.floatValue());
                } else if (QUERY_TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.queryText(parser.text());
                } else if (MODEL_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.modelId(parser.text());
                } else if (MAX_TOKEN_SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.maxTokenScore(parser.floatValue());
                } else if (SEARCH_CLUSTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.searchCluster(parser.booleanValue());
                } else if (DOCUMENT_RATIO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.documentRatio(parser.floatValue());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "[%s] query does not support [%s] field", NAME, currentFieldName)
                    );
                }
            } else if (QUERY_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                Map<String, Float> queryTokens = parser.map(HashMap::new, XContentParser::floatValue);
                sparseEncodingQueryBuilder.queryTokensSupplier(() -> queryTokens);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(Locale.ROOT, "[%s] unknown token [%s] after [%s]", NAME, token, currentFieldName)
                );
            }
        }
    }

    private List<String> getClusterIds(Map<String, Float> queryTokens) {
        // step 1; transform query tokens to a vector
        // assume that the key of queryTokens is a number
        float[] query = new float[30109];
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            query[Integer.parseInt(entry.getKey())] = entry.getValue();
        }
        // step 2: transform query tokens to sketch
        float[] querySketch = new float[SKETCH_SIZE];
        for (int i = 0; i < query.length; i++) {
            querySketch[i % SKETCH_SIZE] = max(querySketch[i % SKETCH_SIZE], query[i]);
        }
        // step 3: call cluster service to get top clusters with ratio
        Integer[] topClusters = DocumentClusterManager.getInstance().getTopClusters(querySketch, this.documentRatio);

        return Arrays.stream(topClusters).map(id -> "cluster_" + id).collect(Collectors.toList());
    }

    private Map<String, Float> generateNewQueryTokensBasedOnClusters(Map<String, Float> queryTokens) {
        if (!BooleanUtils.isTrue(this.searchCluster)) {
            return queryTokens;
        }
        final List<String> clusterIds = getClusterIds(queryTokens);
        Map<String, Float> newQueryTokens = new HashMap<>();
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            for (String clusterId : clusterIds) {
                String newToken = DocumentClusterUtils.constructNewToken(entry.getKey(), clusterId);
                newQueryTokens.put(newToken, entry.getValue());
            }
        }
        return newQueryTokens;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // We need to inference the sentence to get the queryTokens. The logic is similar to NeuralQueryBuilder
        // If the inference is finished, then rewrite to self and call doToQuery, otherwise, continue doRewrite
        // QueryTokensSupplier means 2 case now,
        // 1. It's the queryBuilder built for two-phase, doesn't need any rewrite.
        // 2. It's registerAsyncAction has been registered successful.
        if (Objects.nonNull(queryTokensSupplier)) {
            return this;
        }
        validateForRewrite(queryText, modelId);
        SetOnce<Map<String, Float>> queryTokensSetOnce = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(getModelInferenceAsync(queryTokensSetOnce));
        return new NeuralSparseQueryBuilder().fieldName(fieldName)
            .queryText(queryText)
            .modelId(modelId)
            .maxTokenScore(maxTokenScore)
            .queryTokensSupplier(queryTokensSetOnce::get)
            .twoPhaseSharedQueryToken(twoPhaseSharedQueryToken)
            .twoPhasePruneRatio(twoPhasePruneRatio);
    }

    private BiConsumer<Client, ActionListener<?>> getModelInferenceAsync(SetOnce<Map<String, Float>> setOnce) {
        // When Two-phase shared query tokens is null,
        // it set queryTokensSupplier to the inference result which has all query tokens with score.
        // When Two-phase shared query tokens exist,
        // it splits the tokens using a threshold defined by a ratio of the maximum score of tokens, updating the token set
        // accordingly.
        return ((client, actionListener) -> ML_CLIENT.inferenceSentencesWithMapResult(
            modelId(),
            List.of(queryText),
            ActionListener.wrap(mapResultList -> {
                Map<String, Float> queryTokens = TokenWeightUtil.fetchListOfTokenWeightMap(mapResultList).get(0);
                if (Objects.nonNull(twoPhaseSharedQueryToken)) {
                    Tuple<Map<String, Float>, Map<String, Float>> splitQueryTokens = splitQueryTokensByRatioedMaxScoreAsThreshold(
                        queryTokens,
                        twoPhasePruneRatio
                    );
                    setOnce.set(splitQueryTokens.v1());
                    twoPhaseSharedQueryToken = splitQueryTokens.v2();
                } else {
                    setOnce.set(queryTokens);
                }
                actionListener.onResponse(null);
            }, actionListener::onFailure)
        ));
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(fieldName);
        validateFieldType(ft);
        Map<String, Float> queryTokens = generateNewQueryTokensBasedOnClusters(queryTokensSupplier.get());
        if (Objects.isNull(queryTokens)) {
            throw new IllegalArgumentException("Query tokens cannot be null.");
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            builder.add(FeatureField.newLinearQuery(fieldName, entry.getKey(), entry.getValue()), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    private static void validateForRewrite(String queryText, String modelId) {
        if (StringUtils.isBlank(queryText) || StringUtils.isBlank(modelId)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "%s and %s cannot be null",
                    QUERY_TEXT_FIELD.getPreferredName(),
                    MODEL_ID_FIELD.getPreferredName()
                )
            );
        }
    }

    private static void validateFieldType(MappedFieldType fieldType) {
        if (Objects.isNull(fieldType) || !fieldType.typeName().equals("rank_features")) {
            throw new IllegalArgumentException("[" + NAME + "] query only works on [rank_features] fields");
        }
    }

    @Override
    protected boolean doEquals(NeuralSparseQueryBuilder obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        if (Objects.isNull(queryTokensSupplier) && Objects.nonNull(obj.queryTokensSupplier)) {
            return false;
        }
        if (Objects.nonNull(queryTokensSupplier) && Objects.isNull(obj.queryTokensSupplier)) {
            return false;
        }

        EqualsBuilder equalsBuilder = new EqualsBuilder().append(fieldName, obj.fieldName)
            .append(queryText, obj.queryText)
            .append(modelId, obj.modelId)
            .append(maxTokenScore, obj.maxTokenScore)
            .append(twoPhasePruneRatio, obj.twoPhasePruneRatio)
            .append(twoPhaseSharedQueryToken, obj.twoPhaseSharedQueryToken)
            .append(searchCluster, obj.searchCluster)
            .append(documentRatio, obj.documentRatio);
        if (Objects.nonNull(queryTokensSupplier)) {
            equalsBuilder.append(queryTokensSupplier.get(), obj.queryTokensSupplier.get());
        }
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(fieldName)
            .append(queryText)
            .append(modelId)
            .append(maxTokenScore)
            .append(twoPhasePruneRatio)
            .append(twoPhaseSharedQueryToken)
            .append(searchCluster)
            .append(documentRatio);
        if (Objects.nonNull(queryTokensSupplier)) {
            builder.append(queryTokensSupplier.get());
        }
        return builder.toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static boolean isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID);
    }

}
