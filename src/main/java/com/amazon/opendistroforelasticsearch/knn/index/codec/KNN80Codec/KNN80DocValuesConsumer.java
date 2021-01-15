/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.knn.index.codec.KNN80Codec;

import com.amazon.opendistroforelasticsearch.knn.index.SpaceTypes;
import com.amazon.opendistroforelasticsearch.knn.index.codec.KNNCodecUtil;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import com.amazon.opendistroforelasticsearch.knn.index.KNNSettings;
import com.amazon.opendistroforelasticsearch.knn.index.KNNVectorFieldMapper;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import com.amazon.opendistroforelasticsearch.knn.index.util.NmsLibVersion;
import com.amazon.opendistroforelasticsearch.knn.index.v2011.KNNIndex;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class writes the KNN docvalues to the segments
 */
class KNN80DocValuesConsumer extends DocValuesConsumer implements Closeable {

    private final Logger logger = LogManager.getLogger(KNN80DocValuesConsumer.class);

    private final String TEMP_SUFFIX = "tmp";
    private DocValuesConsumer delegatee;
    private SegmentWriteState state;

    KNN80DocValuesConsumer(DocValuesConsumer delegatee, SegmentWriteState state) throws IOException {
        this.delegatee = delegatee;
        this.state = state;
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addBinaryField(field, valuesProducer);
        addKNNBinaryField(field, valuesProducer);
    }

    public void addKNNBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        KNNCounter.GRAPH_INDEX_REQUESTS.increment();
        if (field.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {

            /**
             * We always write with latest NMS library version
             */
            if (!isNmsLibLatest()) {
                KNNCounter.GRAPH_INDEX_ERRORS.increment();
                throw new IllegalStateException("Nms library version mismatch. Correct version: "
                                                        + NmsLibVersion.LATEST.indexLibraryVersion());
            }

            BinaryDocValues values = valuesProducer.getBinary(field);
            String hnswFileName = String.format("%s_%s_%s%s", state.segmentInfo.name, NmsLibVersion.LATEST.buildVersion,
                    field.name, KNNCodecUtil.HNSW_EXTENSION);
            String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(state.directory))).getDirectory().toString(),
                    hnswFileName).toString();

            KNNCodecUtil.Pair pair = KNNCodecUtil.getFloats(values);
            if (pair == null || pair.vectors.length == 0 || pair.docs.length == 0) {
                logger.info("Skipping hnsw index creation as there are no vectors or docs in the documents");
                return;
            }

            // Pass the path for the nms library to save the file
            String tempIndexPath = indexPath + TEMP_SUFFIX;
            Map<String, String> fieldAttributes = field.attributes();
            String spaceType = fieldAttributes.getOrDefault(KNNConstants.SPACE_TYPE, SpaceTypes.l2.getValue());
            String[] algoParams = getKNNIndexParams(fieldAttributes);
            AccessController.doPrivileged(
                    new PrivilegedAction<Void>() {
                        public Void run() {
                            KNNIndex.saveIndex(pair.docs, pair.vectors, tempIndexPath, algoParams, spaceType);
                            return null;
                        }
                    }
            );

            String hsnwTempFileName = hnswFileName + TEMP_SUFFIX;

            /**
             * Adds Footer to the serialized graph
             * 1. Copies the serialized graph to new file.
             * 2. Adds Footer to the new file.
             *
             * We had to create new file here because adding footer directly to the
             * existing file will miss calculating checksum for the serialized graph
             * bytes and result in index corruption issues.
             */
            try (IndexInput is = state.directory.openInput(hsnwTempFileName, state.context);
                 IndexOutput os = state.directory.createOutput(hnswFileName, state.context)) {
                os.copyBytes(is, is.length());
                CodecUtil.writeFooter(os);
            } catch (Exception ex) {
                KNNCounter.GRAPH_INDEX_ERRORS.increment();
                throw new RuntimeException("[KNN] Adding footer to serialized graph failed: " + ex);
            } finally {
                IOUtils.deleteFilesIgnoringExceptions(state.directory, hsnwTempFileName);
            }
        }
    }

    /**
     * Merges in the fields from the readers in mergeState
     *
     * @param mergeState Holds common state used during segment merging
     */
    @Override
    public void merge(MergeState mergeState) {
        try {
            delegatee.merge(mergeState);
            assert mergeState != null;
            assert mergeState.mergeFieldInfos != null;
            for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
                DocValuesType type = fieldInfo.getDocValuesType();
                if (type == DocValuesType.BINARY) {
                    addKNNBinaryField(fieldInfo, new KNN80DocValuesReader(mergeState));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addSortedSetField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addSortedField(field, valuesProducer);
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegatee.addNumericField(field, valuesProducer);
    }

    @Override
    public void close() throws IOException {
        delegatee.close();
    }

    private boolean isNmsLibLatest() {
        return AccessController.doPrivileged(
                new PrivilegedAction<Boolean>() {
                    public Boolean run() {
                        if (!NmsLibVersion.LATEST.indexLibraryVersion().equals(KNNIndex.VERSION.indexLibraryVersion())) {
                            String errorMessage = String.format("KNN codec nms library version mis match. Latest version: %s" +
                                                                        "Current version: %s",
                                    NmsLibVersion.LATEST.indexLibraryVersion(), KNNIndex.VERSION);
                            logger.error(errorMessage);
                            return false;
                        }
                        return true;
                    }
                }
        );
    }

    private String[] getKNNIndexParams(Map<String, String> fieldAttributes) {
        List<String> algoParams = new ArrayList<>();
        if (fieldAttributes.containsKey(KNNConstants.HNSW_ALGO_M)) {
            algoParams.add(KNNConstants.HNSW_ALGO_M + "=" + fieldAttributes.get(KNNConstants.HNSW_ALGO_M));
        }
        if (fieldAttributes.containsKey(KNNConstants.HNSW_ALGO_EF_CONSTRUCTION)) {
            algoParams.add(KNNConstants.HNSW_ALGO_EF_CONSTRUCTION + "=" + fieldAttributes.get(KNNConstants.HNSW_ALGO_EF_CONSTRUCTION));
        }
        
        // Cluster level setting so no need to specify for every index creation
        algoParams.add(KNNConstants.HNSW_ALGO_INDEX_THREAD_QTY + "=" + KNNSettings.state().getSettingValue(
                KNNSettings.KNN_ALGO_PARAM_INDEX_THREAD_QTY));
        return algoParams.toArray(new String[0]);
    }
}
