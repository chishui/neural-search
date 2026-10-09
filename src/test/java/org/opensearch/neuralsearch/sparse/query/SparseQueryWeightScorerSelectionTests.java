/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;

/**
 * Which of the two native-engine scorers a segment gets. The setting is left at its default (off)
 * throughout, since {@link AbstractSparseTestBase} resets the settings singleton per test -- so what is
 * pinned here is the automatic half of the decision.
 */
public class SparseQueryWeightScorerSelectionTests extends AbstractSparseTestBase {

    /**
     * A store from which no filesystem path can be resolved leaves nsparse nothing to map, so the query
     * has to read the file itself rather than fail.
     */
    public void testFallsBackWhenTheStoreHasNoFilesystemPath() {
        // TestsPrepareUtils builds a ByteBuffersDirectory, which unwraps to no FSDirectory
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo(10);

        assertTrue(SparseQueryWeight.shouldSearchThroughIndexInput(perBlockSeismicField(), segmentInfo));
    }

    /** With a real path and the setting off, the native path stays the default. */
    @SneakyThrows
    public void testKeepsTheNativePathForAFilesystemStore() {
        try (Directory directory = FSDirectory.open(createTempDir())) {
            assertFalse(SparseQueryWeight.shouldSearchThroughIndexInput(perBlockSeismicField(), segmentInfo(directory, 10)));
        }
    }

    /** The reader parses disk_seismic_sq only, which is what a per_block field builds. */
    public void testNotForASharedForwardIndex() {
        FieldInfo fieldInfo = perBlockSeismicField();
        when(fieldInfo.getAttribute(FORWARD_INDEX_FIELD)).thenReturn(SparseForwardIndex.SHARED.getName());

        assertFalse(SparseQueryWeight.shouldSearchThroughIndexInput(fieldInfo, TestsPrepareUtils.prepareSegmentInfo(10)));
    }

    /**
     * Below its approximate threshold a segment gets an nsparse {@code inverted} index instead, which
     * the reader does not parse -- so the native path has to keep it even though it also has no path to
     * map here.
     */
    public void testNotBelowTheApproximateThreshold() {
        FieldInfo fieldInfo = perBlockSeismicField();
        when(fieldInfo.attributes()).thenReturn(Map.of(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(Integer.MAX_VALUE)));

        assertFalse(SparseQueryWeight.shouldSearchThroughIndexInput(fieldInfo, TestsPrepareUtils.prepareSegmentInfo(10)));
    }

    public void testNotWithoutASegment() {
        assertFalse(SparseQueryWeight.shouldSearchThroughIndexInput(perBlockSeismicField(), null));
    }

    private FieldInfo perBlockSeismicField() {
        FieldInfo fieldInfo = mock(FieldInfo.class);
        when(fieldInfo.getAttribute(FORWARD_INDEX_FIELD)).thenReturn(SparseForwardIndex.PER_BLOCK.getName());
        when(fieldInfo.attributes()).thenReturn(Map.of(APPROXIMATE_THRESHOLD_FIELD, "1"));
        return fieldInfo;
    }

    private SegmentInfo segmentInfo(Directory directory, int maxDoc) {
        return new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_0",
            maxDoc,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            new byte[StringHelper.ID_LENGTH],
            Collections.emptyMap(),
            null
        );
    }
}
