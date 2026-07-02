/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;

public abstract class SparseVectorBinaryConsumer {
    abstract public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;

    abstract public void merge(List<FieldInfo> fieldInfos, MergeStateFacade facade) throws IOException;
}
