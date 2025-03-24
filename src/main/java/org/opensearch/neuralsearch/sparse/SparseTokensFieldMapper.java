/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.Getter;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.Query;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SourceValueFetcher;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class SparseTokensFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "sparse_tokens";
    private final int sparseDimension;
    private final int sketchDimension;
    private final String sketchingAlgorithm;

    private SparseTokensFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        int sparseDimension,
        int sketchDimension,
        String sketchingAlgorithm
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.sketchDimension = sketchDimension;
        this.sparseDimension = sparseDimension;
        this.sketchingAlgorithm = sketchingAlgorithm;
    }

    private static SparseTokensFieldType ft(FieldMapper in) {
        return ((SparseTokensFieldMapper) in).fieldType();
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static class Builder extends ParametrizedFieldMapper.Builder {
        private final Parameter<Integer> sparseDimension = Parameter.intParam("sparse_dimension", false, m -> ft(m).sparseDimension, 1);

        private final Parameter<Integer> sketchDimension = Parameter.intParam("sketch_dimension", false, m -> ft(m).sketchDimension, 1);

        private final Parameter<String> sketchingAlgorithm = Parameter.restrictedStringParam(
            "sketching_algorithm",
            false,
            m -> ft(m).sketchingAlgorithm,
            "jlt"
        );

        /**
         * Creates a new Builder with a field name
         *
         * @param name
         */
        protected Builder(String name) {
            super(name);
            builder = this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(sparseDimension, sketchDimension, sketchingAlgorithm);
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            return new SparseTokensFieldMapper(
                name,
                new SparseTokensFieldType(
                    buildFullName(context),
                    sparseDimension.getValue(),
                    sketchDimension.getValue(),
                    sketchingAlgorithm.getValue()
                ),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                sparseDimension.getValue(),
                sketchDimension.getValue(),
                sketchingAlgorithm.getValue()
            );
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected SparseTokensFieldMapper clone() {
        return (SparseTokensFieldMapper) super.clone();
    }

    @Override
    public SparseTokensFieldType fieldType() {
        return (SparseTokensFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("[" + CONTENT_TYPE + "] fields can't be used in multi-fields");
        }

        if (context.parser().currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[" + CONTENT_TYPE + "] fields must be json objects, expected a START_OBJECT but got: " + context.parser().currentToken()
            );
        }

        String feature = null;
        for (XContentParser.Token token = context.parser().nextToken(); token != XContentParser.Token.END_OBJECT; token = context.parser()
            .nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                feature = context.parser().currentName();
            } else if (token == XContentParser.Token.VALUE_NULL) {
                // ignore feature, this is consistent with numeric fields
            } else if (token == XContentParser.Token.VALUE_NUMBER || token == XContentParser.Token.VALUE_STRING) {
                final String key = name() + "." + feature;
                float value = context.parser().floatValue(true);
                if (context.doc().getByKey(key) != null) {
                    throw new IllegalArgumentException(
                        "["
                            + CONTENT_TYPE
                            + "] fields do not support indexing multiple values for the same "
                            + "rank feature ["
                            + key
                            + "] in the same document"
                    );
                }
                // if (positiveScoreImpact == false) {
                // value = 1 / value;
                // }
                context.doc().addWithKey(key, new FeatureField(name(), feature, value));
            } else {
                throw new IllegalArgumentException(
                    "["
                        + CONTENT_TYPE
                        + "] fields take hashes that map a feature to a strictly positive "
                        + "float, but got unexpected token "
                        + token
                );
            }
        }
    }

    @Getter
    public static final class SparseTokensFieldType extends MappedFieldType {
        private final int sparseDimension;
        private final int sketchDimension;
        private final String sketchingAlgorithm;

        public SparseTokensFieldType(String name, int sparseDimension, int sketchDimension, String sketchingAlgorithm) {
            super(name, false, false, false, TextSearchInfo.NONE, Collections.EMPTY_MAP);
            this.sketchDimension = sketchDimension;
            this.sparseDimension = sparseDimension;
            this.sketchingAlgorithm = sketchingAlgorithm;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Queries on [" + CONTENT_TYPE + "] fields are not supported");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new IllegalArgumentException("[" + CONTENT_TYPE + "] fields do not support [exists] queries");
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            throw new IllegalArgumentException("[" + CONTENT_TYPE + "] fields do not support sorting, scripting or aggregating");
        }
    }
}
