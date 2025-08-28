/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedPostingsReader;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseTokensField.SPARSE_FIELD;

public class SparsePostingsReaderTests extends AbstractSparseTestBase {

    @Mock
    private ThreadPool mockThreadPool;
    @Mock
    private ExecutorService mockExecutor;

    @Mock
    private Terms mockTerms;
    @Mock
    private TermsEnum mockTermsEnum;

    @Mock
    private SparseTerms mockSparseTerms;
    @Mock
    private CacheGatedPostingsReader mockCacheGatedPostingsReader;

    @Mock
    private FieldsProducer mockFieldsProducer;
    @Mock
    private SparseTermsLuceneWriter mockSparseTermsWriter;
    @Mock
    private ClusteredPostingTermsWriter mockClusteredWriter;

    @Mock
    private SparsePostingsEnum mockSparsePostingsEnum;
    @Mock
    private PostingClusters mockPostingClusters;

    private FieldInfo mockFieldInfo;
    private MergeState mockMergeState;
    private SparsePostingsReader reader;
    private static final BytesRef term = new BytesRef("term");
    private static final Set<BytesRef> terms = Set.of(new BytesRef("term"));

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // configure executor service for cluster training running
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(mockExecutor).execute(any(Runnable.class));

        when(mockThreadPool.executor(anyString())).thenReturn(mockExecutor);
        ClusterTrainingExecutor.getInstance().initialize(mockThreadPool);

        // configure sparse term
        when(mockSparseTerms.getReader()).thenReturn(mockCacheGatedPostingsReader);
        when(mockCacheGatedPostingsReader.read(any(BytesRef.class))).thenReturn(mockPostingClusters);
        when(mockCacheGatedPostingsReader.getTerms()).thenReturn(terms);

        // configure non sparse term
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);
        when(mockTermsEnum.next()).thenReturn(term).thenReturn(null);

        // configure merge state
        mockFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        mockMergeState = TestsPrepareUtils.prepareMergeStateWithPassThroughValues(false);
        for (FieldInfo fieldInfo : mockMergeState.mergeFieldInfos) {
            fieldInfo.putAttribute(SPARSE_FIELD, String.valueOf(true));
            fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(10));
            fieldInfo.putAttribute(CLUSTER_RATIO_FIELD, String.valueOf(0.1f));
            fieldInfo.putAttribute(N_POSTINGS_FIELD, String.valueOf(-1));
            fieldInfo.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, String.valueOf(0.4f));
        }
        mockMergeState.fieldsProducers[0] = mockFieldsProducer;

        reader = new SparsePostingsReader(mockMergeState);
    }

    public void testConstructor() {
        SparsePostingsReader testReader = new SparsePostingsReader(mockMergeState);
        assertNotNull(testReader);
    }

    @SneakyThrows
    public void testMerge_success() {
        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockSparseTerms);

        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(1);
        verify(mockSparseTermsWriter, times(1)).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, times(1)).writeTermsSize(1L);
        verify(mockExecutor, times(1)).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_withNonDefaultNPosting() {
        MergeState mockMergeState = TestsPrepareUtils.prepareMergeStateWithPassThroughValues(false);
        for (FieldInfo fieldInfo : mockMergeState.mergeFieldInfos) {
            fieldInfo.putAttribute(SPARSE_FIELD, String.valueOf(true));
            fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(10));
            fieldInfo.putAttribute(CLUSTER_RATIO_FIELD, String.valueOf(0.1f));
            fieldInfo.putAttribute(N_POSTINGS_FIELD, String.valueOf(200));
            fieldInfo.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, String.valueOf(0.4f));
        }
        mockMergeState.fieldsProducers[0] = mockFieldsProducer;
        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockSparseTerms);

        SparsePostingsReader reader = new SparsePostingsReader(mockMergeState);
        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(1);
        verify(mockSparseTermsWriter, times(1)).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, times(1)).writeTermsSize(1L);
        verify(mockExecutor, times(1)).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_withNonSparseTerm() {
        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockTerms);

        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(1);
        verify(mockSparseTermsWriter, times(1)).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, times(1)).writeTermsSize(1L);
        verify(mockExecutor, times(1)).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_withNonPassThrough() {
        MergeState mockMergeState = TestsPrepareUtils.prepareMergeState(true);
        for (FieldInfo fieldInfo : mockMergeState.mergeFieldInfos) {
            fieldInfo.putAttribute(SPARSE_FIELD, String.valueOf(true));
            fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(10));
            fieldInfo.putAttribute(CLUSTER_RATIO_FIELD, String.valueOf(0.0f));
            fieldInfo.putAttribute(N_POSTINGS_FIELD, String.valueOf(-1));
            fieldInfo.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, String.valueOf(0.4f));
        }

        SparsePostingsReader reader = new SparsePostingsReader(mockMergeState);
        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(1);
        verify(mockSparseTermsWriter, times(1)).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, times(1)).writeTermsSize(0);
        verify(mockExecutor, never()).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_withZeroClusterRatio() {
        for (FieldInfo fieldInfo : mockMergeState.mergeFieldInfos) {
            fieldInfo.putAttribute(CLUSTER_RATIO_FIELD, String.valueOf(0.0f));
        }

        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockSparseTerms);

        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(1);
        verify(mockSparseTermsWriter, times(1)).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, times(1)).writeTermsSize(1L);
        verify(mockExecutor, never()).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_withIOException() {
        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockSparseTerms);
        when(mockClusteredWriter.write(any(BytesRef.class), any(PostingClusters.class))).thenThrow(new IOException("Test exception"));

        Exception exception = expectThrows(IOException.class, () -> reader.merge(mockSparseTermsWriter, mockClusteredWriter));
        assertEquals("Test exception", exception.getMessage());
        verify(mockSparseTermsWriter, times(1)).closeWithException();
        verify(mockClusteredWriter, times(1)).closeWithException();
        verify(mockExecutor, times(1)).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_success() {
        int expectedFreq = 5;
        int oldDocId = 1, newDocId = 10;

        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockTerms);
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);

        when(mockTermsEnum.seekExact(any(BytesRef.class))).thenReturn(true);
        when(mockTermsEnum.postings(null)).thenReturn(mockSparsePostingsEnum);
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(oldDocId).thenReturn(PostingsEnum.NO_MORE_DOCS);
        when(mockSparsePostingsEnum.freq()).thenReturn(expectedFreq);

        MergeState.DocMap mockDocMap = mock(MergeState.DocMap.class);
        when(mockDocMap.get(oldDocId)).thenReturn(newDocId);
        mockMergeState.docMaps[0] = mockDocMap;

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        List<DocWeight> result = SparsePostingsReader.getMergedPostingForATerm(
            mockMergeState,
            term,
            mockFieldInfo,
            newIdToFieldProducerIndex,
            newIdToOldId
        );

        assertEquals(1, result.size());
        assertEquals(newDocId, result.get(0).getDocID());
        assertEquals(expectedFreq, result.get(0).getIntWeight());
        assertEquals(0, newIdToFieldProducerIndex[newDocId]);
        assertEquals(oldDocId, newIdToOldId[newDocId]);
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_withNonPassThrough() {
        MergeState mockMergeState = TestsPrepareUtils.prepareMergeState(true);
        for (FieldInfo fieldInfo : mockMergeState.mergeFieldInfos) {
            fieldInfo.putAttribute(SPARSE_FIELD, String.valueOf(true));
            fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(10));
            fieldInfo.putAttribute(CLUSTER_RATIO_FIELD, String.valueOf(0.0f));
            fieldInfo.putAttribute(N_POSTINGS_FIELD, String.valueOf(-1));
            fieldInfo.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, String.valueOf(0.4f));
        }

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        List<DocWeight> result = SparsePostingsReader.getMergedPostingForATerm(
            mockMergeState,
            term,
            mockFieldInfo,
            newIdToFieldProducerIndex,
            newIdToOldId
        );

        assertTrue(result.isEmpty());
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_withNullTerms() {
        when(mockFieldsProducer.terms(any(String.class))).thenReturn(null);

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        List<DocWeight> result = SparsePostingsReader.getMergedPostingForATerm(
            mockMergeState,
            term,
            mockFieldInfo,
            newIdToFieldProducerIndex,
            newIdToOldId
        );

        assertTrue(result.isEmpty());
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_withNullTermsEnum() {
        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockTerms);
        when(mockTerms.iterator()).thenReturn(null);

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        List<DocWeight> result = SparsePostingsReader.getMergedPostingForATerm(
            mockMergeState,
            term,
            mockFieldInfo,
            newIdToFieldProducerIndex,
            newIdToOldId
        );

        assertTrue(result.isEmpty());
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_withTermNotFound() {
        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockTerms);
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);
        when(mockTermsEnum.seekExact(any(BytesRef.class))).thenReturn(false);

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        List<DocWeight> result = SparsePostingsReader.getMergedPostingForATerm(
            mockMergeState,
            term,
            mockFieldInfo,
            newIdToFieldProducerIndex,
            newIdToOldId
        );

        assertTrue(result.isEmpty());
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_withErrorOldDocId() {
        int oldDocId = -1;

        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockTerms);
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);

        when(mockTermsEnum.seekExact(any(BytesRef.class))).thenReturn(true);
        when(mockTermsEnum.postings(null)).thenReturn(mockSparsePostingsEnum);
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(oldDocId).thenReturn(PostingsEnum.NO_MORE_DOCS);

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        List<DocWeight> result = SparsePostingsReader.getMergedPostingForATerm(
            mockMergeState,
            term,
            mockFieldInfo,
            newIdToFieldProducerIndex,
            newIdToOldId
        );

        assertTrue(result.isEmpty());
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_withErrorNewDocId() {
        int oldDocId = 1, newDocId = -1;

        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockTerms);
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);

        when(mockTermsEnum.seekExact(any(BytesRef.class))).thenReturn(true);
        when(mockTermsEnum.postings(null)).thenReturn(mockSparsePostingsEnum);
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(oldDocId).thenReturn(PostingsEnum.NO_MORE_DOCS);

        MergeState.DocMap mockDocMap = mock(MergeState.DocMap.class);
        when(mockDocMap.get(oldDocId)).thenReturn(newDocId);
        mockMergeState.docMaps[0] = mockDocMap;

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        List<DocWeight> result = SparsePostingsReader.getMergedPostingForATerm(
            mockMergeState,
            term,
            mockFieldInfo,
            newIdToFieldProducerIndex,
            newIdToOldId
        );

        assertTrue(result.isEmpty());
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_withOutOfBoundNewDocId() {
        int oldDocId = 1, newDocId = 100;

        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockTerms);
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);

        when(mockTermsEnum.seekExact(any(BytesRef.class))).thenReturn(true);
        when(mockTermsEnum.postings(null)).thenReturn(mockSparsePostingsEnum);
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(oldDocId).thenReturn(PostingsEnum.NO_MORE_DOCS);

        MergeState.DocMap mockDocMap = mock(MergeState.DocMap.class);
        when(mockDocMap.get(oldDocId)).thenReturn(newDocId);
        mockMergeState.docMaps[0] = mockDocMap;

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        Exception exception = expectThrows(
            RuntimeException.class,
            () -> SparsePostingsReader.getMergedPostingForATerm(
                mockMergeState,
                term,
                mockFieldInfo,
                newIdToFieldProducerIndex,
                newIdToOldId
            )
        );

        assertEquals("newDocId is larger than array size!", exception.getMessage());
    }

    @SneakyThrows
    public void testGetMergedPostingForATerm_withNonSparseTerm() {
        int expectedFreq = 100;
        int oldDocId = 1, newDocId = 10;

        PostingsEnum mockPostingsEnum = mock(PostingsEnum.class);
        when(mockFieldsProducer.terms(any(String.class))).thenReturn(mockTerms);
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);
        when(mockTermsEnum.seekExact(any(BytesRef.class))).thenReturn(true);
        when(mockTermsEnum.postings(null)).thenReturn(mockPostingsEnum);
        when(mockPostingsEnum.nextDoc()).thenReturn(oldDocId).thenReturn(PostingsEnum.NO_MORE_DOCS);
        when(mockPostingsEnum.freq()).thenReturn(expectedFreq);

        MergeState.DocMap mockDocMap = mock(MergeState.DocMap.class);
        when(mockDocMap.get(oldDocId)).thenReturn(newDocId);
        mockMergeState.docMaps[0] = mockDocMap;

        int[] newIdToFieldProducerIndex = new int[20];
        int[] newIdToOldId = new int[20];

        List<DocWeight> result = SparsePostingsReader.getMergedPostingForATerm(
            mockMergeState,
            term,
            mockFieldInfo,
            newIdToFieldProducerIndex,
            newIdToOldId
        );

        assertEquals(1, result.size());
        assertEquals(newDocId, result.get(0).getDocID());
        assertEquals(0, newIdToFieldProducerIndex[newDocId]);
        assertEquals(oldDocId, newIdToOldId[newDocId]);
    }
}
