/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergeHelperTests extends AbstractSparseTestBase {

    private MergeStateFacade mergeStateFacade;
    private DocValuesProducer docValuesProducer1;
    private DocValuesProducer docValuesProducer2;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        // final String inlineMockMaker = "org.mockito.internal.creation.bytebuddy.InlineByteBuddyMockMaker";
        // MockSettings mockSettingsWithInlineMockMaker = new MockSettingsImpl().mockMaker(inlineMockMaker);

        mergeStateFacade = mock(MergeStateFacade.class);
        DocValuesProducer docValuesProducer1 = mock(DocValuesProducer.class);
        DocValuesProducer docValuesProducer2 = mock(DocValuesProducer.class);

        FieldInfo fieldInfo1 = mock(FieldInfo.class);
        FieldInfo fieldInfo2 = mock(FieldInfo.class);
        List<FieldInfo> fields = Arrays.asList(fieldInfo1, fieldInfo2);

        FieldInfos fieldInfos = mock(FieldInfos.class);
        when(fieldInfos.iterator()).thenReturn(fields.iterator());
        when(mergeStateFacade.getMergeFieldInfos()).thenReturn(fieldInfos);

    }

    public void testClear() throws IOException {
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1, docValuesProducer2});
        MergeHelper.clearInMemoryData(mergeStateFacade, null, (t) -> {

        });
        assertTrue(true);
    }

}
