/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistroforelasticsearch.knn.index.KNNMethodContext;
import com.amazon.opendistroforelasticsearch.knn.index.SpaceType;
import com.amazon.opendistroforelasticsearch.knn.index.codec.KNNCodecUtil;
import com.amazon.opendistroforelasticsearch.knn.index.faiss.v165.KNNFaissIndex;
import com.amazon.opendistroforelasticsearch.knn.index.util.FaissLibVersion;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNEngine;
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
import com.amazon.opendistroforelasticsearch.knn.common.KNNConstants;
import com.amazon.opendistroforelasticsearch.knn.index.util.NmsLibVersion;
import com.amazon.opendistroforelasticsearch.knn.index.nmslib.v2011.KNNNmsLibIndex;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.FAISS_NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NMSLIB_NAME;
import static com.amazon.opendistroforelasticsearch.knn.index.codec.KNNCodecUtil.buildEngineFileName;

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
             * First Get Attribute from field
             */
            Map<String, String> fieldAttributes = field.attributes();
            String engineName = fieldAttributes.getOrDefault(KNNConstants.KNN_ENGINE, KNNEngine.DEFAULT.getName());
            String spaceType = fieldAttributes.getOrDefault(KNNConstants.SPACE_TYPE, SpaceType.L2.getValue());
            int trainingDatasetSizeLimit = Integer.parseInt(fieldAttributes.getOrDefault(
                    KNNConstants.TRAINING_DATASET_SIZE_LIMIT,
                    KNNMethodContext.DEFAULT_TRAINING_DATASET_SIZE_LIMIT.toString()));
            int minimumDatapoints = Integer.parseInt(fieldAttributes.getOrDefault(
                    KNNConstants.MINIMUM_DATAPOINTS, KNNMethodContext.DEFAULT_MINIMUM_DATAPOINTS.toString()));

            String[] algoParams = getKNNIndexParams(fieldAttributes);
            KNNEngine knnEngine = KNNEngine.getEngine(engineName);

            String method = fieldAttributes.get(KNNConstants.KNN_METHOD);
            if (method == null) {
                throw new NullPointerException("Method cannot be null");
            }

            /**
             * We always write with latest engine version
             */
            if (!isKnnLibLatest()) {
                KNNCounter.GRAPH_INDEX_ERRORS.increment();
                throw new IllegalStateException("KNN library version mismatch. Correct version: " + NMSLIB_NAME + ": "
                        + NmsLibVersion.VNMSLIB_2011.indexLibraryVersion() + FAISS_NAME + ": "
                        + FaissLibVersion.VFAISS_165.indexLibraryVersion());
            }

            /**
             * Make Engine Name Into FileName
             */
            BinaryDocValues values = valuesProducer.getBinary(field);
            String engineFileName = buildEngineFileName(state.segmentInfo.name, knnEngine.getLatestBuildVersion(),
                    field.name, knnEngine.getExtension());
            String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(state.directory))).getDirectory().toString(),
                    engineFileName).toString();

            KNNCodecUtil.Pair pair = KNNCodecUtil.getFloats(values);
            if (pair.vectors.length == 0 || pair.docs.length == 0) {
                logger.info("Skipping engine index creation as there are no vectors or docs in the documents");
                return;
            }

            // Pass the path for the nms library to save the file
            String tempIndexPath = indexPath + TEMP_SUFFIX;
            AccessController.doPrivileged(
                    (PrivilegedAction<Void>) () -> {
                        if(KNNEngine.NMSLIB.getName().equals(knnEngine.getName())) {
                            KNNNmsLibIndex.save(pair.docs, pair.vectors, tempIndexPath, algoParams, spaceType,
                                    method);
                            return null;
                        }
                        if (KNNEngine.FAISS.getName().equals(knnEngine.getName())) {
                            String extraParametersString = fieldAttributes.getOrDefault(KNNConstants.EXTRA_PARAMETERS,
                                    null);
                            Map<String, Object> extraParameterMap = Collections.emptyMap();
                            if (extraParametersString != null) {
                                try {
                                    extraParameterMap = XContentFactory.xContent(XContentType.JSON)
                                            .createParser(NamedXContentRegistry.EMPTY,
                                                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                                    extraParametersString)
                                            .map();
                                } catch (IOException e) {
                                    throw new IllegalStateException(e);
                                }

                            }

                            KNNFaissIndex.save(pair.docs, pair.vectors, tempIndexPath, extraParameterMap, spaceType,
                                    method, trainingDatasetSizeLimit, minimumDatapoints);
                            return null;
                        }
                        throw new IllegalStateException("Invalid engine: "+knnEngine.getName());
                    }
            );

            String engineTempFileName = engineFileName + TEMP_SUFFIX;

            /*
             * Adds Footer to the serialized graph
             * 1. Copies the serialized graph to new file.
             * 2. Adds Footer to the new file.
             *
             * We had to create new file here because adding footer directly to the
             * existing file will miss calculating checksum for the serialized graph
             * bytes and result in index corruption issues.
             */
            try (IndexInput is = state.directory.openInput(engineTempFileName, state.context);
                 IndexOutput os = state.directory.createOutput(engineFileName, state.context)) {
                os.copyBytes(is, is.length());
                CodecUtil.writeFooter(os);
            } catch (Exception ex) {
                KNNCounter.GRAPH_INDEX_ERRORS.increment();
                throw new RuntimeException("[KNN] Adding footer to serialized graph failed: " + ex);
            } finally {
                IOUtils.deleteFilesIgnoringExceptions(state.directory, engineTempFileName);
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

    private boolean isKnnLibLatest() {
        return AccessController.doPrivileged(
                (PrivilegedAction<Boolean>) () -> {
                    if (!(NmsLibVersion.VNMSLIB_2011.indexLibraryVersion().equals(KNNNmsLibIndex.VERSION.indexLibraryVersion()) &&
                            FaissLibVersion.VFAISS_165.indexLibraryVersion().equals(KNNFaissIndex.VERSION.indexLibraryVersion()))) {
                        String errorMessage = String.format("KNN codec nms library version mis match. Latest version: %s" +
                                        "Current version: %s, %s",
                                NmsLibVersion.VNMSLIB_2011.indexLibraryVersion(), KNNNmsLibIndex.VERSION, KNNFaissIndex.VERSION);
                        logger.error(errorMessage);
                        return false;
                    }
                    return true;
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
