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

package com.amazon.opendistroforelasticsearch.knn.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.Property.Dynamic;
import static org.elasticsearch.common.settings.Setting.Property.IndexScope;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;

/**
 * This class defines
 * 1. KNN settings to hold the HNSW algorithm parameters.
 * https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md
 * 2. KNN settings to enable/disable plugin, circuit breaker settings
 * 3. KNN settings to manage graphs loaded in native memory
 */
public class KNNSettings {

    private static Logger logger = LogManager.getLogger(KNNSettings.class);
    private static KNNSettings INSTANCE;
    private static OsProbe osProbe = OsProbe.getInstance();

    private static final int INDEX_THREAD_QTY_MAX = 32;

    /**
     * Settings name
     */
    public static final String KNN_SPACE_TYPE = "index.knn.space_type";
    public static final String KNN_ALGO_PARAM_M = "index.knn.algo_param.m";
    public static final String KNN_ALGO_PARAM_EF_CONSTRUCTION = "index.knn.algo_param.ef_construction";
    public static final String KNN_ALGO_PARAM_EF_SEARCH = "index.knn.algo_param.ef_search";
    public static final String KNN_ALGO_PARAM_INDEX_THREAD_QTY = "knn.algo_param.index_thread_qty";
    public static final String KNN_MEMORY_CIRCUIT_BREAKER_ENABLED = "knn.memory.circuit_breaker.enabled";
    public static final String KNN_MEMORY_CIRCUIT_BREAKER_LIMIT = "knn.memory.circuit_breaker.limit";
    public static final String KNN_CIRCUIT_BREAKER_TRIGGERED = "knn.circuit_breaker.triggered";
    public static final String KNN_CACHE_ITEM_EXPIRY_ENABLED = "knn.cache.item.expiry.enabled";
    public static final String KNN_CACHE_ITEM_EXPIRY_TIME_MINUTES = "knn.cache.item.expiry.minutes";
    public static final String KNN_PLUGIN_ENABLED = "knn.plugin.enabled";
    public static final String KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE = "knn.circuit_breaker.unset.percentage";
    public static final String KNN_INDEX = "index.knn";

    /**
     * Default setting values
     */
    public static final String INDEX_KNN_DEFAULT_SPACE_TYPE = "l2";
    public static final Integer INDEX_KNN_DEFAULT_ALGO_PARAM_M = 16;
    public static final Integer INDEX_KNN_DEFAULT_ALGO_PARAM_EF_SEARCH = 512;
    public static final Integer INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION = 512;
    public static final Integer KNN_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY = 1;
    public static final Integer KNN_DEFAULT_CIRCUIT_BREAKER_UNSET_PERCENTAGE = 75;

    /**
     * Settings Definition
     */

    public static final Setting<String> INDEX_KNN_SPACE_TYPE = Setting.simpleString(KNN_SPACE_TYPE,
            INDEX_KNN_DEFAULT_SPACE_TYPE,
        new SpaceTypeValidator(),
        IndexScope);

    /**
     * M - the number of bi-directional links created for every new element during construction.
     * Reasonable range for M is 2-100. Higher M work better on datasets with high intrinsic
     * dimensionality and/or high recall, while low M work better for datasets with low intrinsic dimensionality and/or low recalls.
     * The parameter also determines the algorithm's memory consumption, which is roughly M * 8-10 bytes per stored element.
     */
    public static final Setting<Integer> INDEX_KNN_ALGO_PARAM_M_SETTING =  Setting.intSetting(KNN_ALGO_PARAM_M,
            INDEX_KNN_DEFAULT_ALGO_PARAM_M,
            2,
            IndexScope);

    /**
     *  ef or efSearch - the size of the dynamic list for the nearest neighbors (used during the search).
     *  Higher ef leads to more accurate but slower search. ef cannot be set lower than the number of queried nearest neighbors k.
     *  The value ef can be anything between k and the size of the dataset.
     */
    public static final Setting<Integer> INDEX_KNN_ALGO_PARAM_EF_SEARCH_SETTING =  Setting.intSetting(KNN_ALGO_PARAM_EF_SEARCH,
            INDEX_KNN_DEFAULT_ALGO_PARAM_EF_SEARCH,
            2,
            IndexScope,
            Dynamic);

    /**
     * ef_constrution - the parameter has the same meaning as ef, but controls the index_time/index_accuracy.
     * Bigger ef_construction leads to longer construction(more indexing time), but better index quality.
     */
    public static final Setting<Integer> INDEX_KNN_ALGO_PARAM_EF_CONSTRUCTION_SETTING =  Setting.intSetting(KNN_ALGO_PARAM_EF_CONSTRUCTION,
            INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION,
            2,
            IndexScope);

    /**
     * This setting identifies KNN index.
     */
    public static final Setting<Boolean> IS_KNN_INDEX_SETTING =  Setting.boolSetting(KNN_INDEX, false, IndexScope);


    /**
     * index_thread_quantity - the parameter specifies how many threads the nms library should use to create the graph.
     * By default, the nms library sets this value to NUM_CORES. However, because ES can spawn NUM_CORES threads for
     * indexing, and each indexing thread calls the NMS library to build the graph, which can also spawn NUM_CORES threads,
     * this could lead to NUM_CORES^2 threads running and could lead to 100% CPU utilization. This setting allows users to
     * configure number of threads for graph construction.
     */
    public static final Setting<Integer> KNN_ALGO_PARAM_INDEX_THREAD_QTY_SETTING =  Setting.intSetting(KNN_ALGO_PARAM_INDEX_THREAD_QTY,
            KNN_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY,
            1,
            INDEX_THREAD_QTY_MAX,
            NodeScope,
            Dynamic);

    public static final Setting<Boolean> KNN_CIRCUIT_BREAKER_TRIGGERED_SETTING =  Setting.boolSetting(KNN_CIRCUIT_BREAKER_TRIGGERED,
            false,
            NodeScope,
            Dynamic);

    public static final Setting<Double> KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE_SETTING =  Setting.doubleSetting(
            KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE,
            KNN_DEFAULT_CIRCUIT_BREAKER_UNSET_PERCENTAGE,
            0,
            100,
            NodeScope,
            Dynamic);
    /**
     * Dynamic settings
     */
    public static  Map<String, Setting<?>> dynamicCacheSettings = new HashMap<String, Setting<?>>() {
        {
            /**
             * KNN plugin enable/disable setting
             */
            put(KNN_PLUGIN_ENABLED, Setting.boolSetting(KNN_PLUGIN_ENABLED, true, NodeScope, Dynamic));

            /**
             * Weight circuit breaker settings
             */
            put(KNN_MEMORY_CIRCUIT_BREAKER_ENABLED, Setting.boolSetting(KNN_MEMORY_CIRCUIT_BREAKER_ENABLED,true,
                    NodeScope, Dynamic));
            put(KNN_MEMORY_CIRCUIT_BREAKER_LIMIT, knnMemoryCircuitBreakerSetting(KNN_MEMORY_CIRCUIT_BREAKER_LIMIT, "50%",
                    NodeScope, Dynamic));

            /**
             * Cache expiry time settings
             */
            put(KNN_CACHE_ITEM_EXPIRY_ENABLED, Setting.boolSetting(KNN_CACHE_ITEM_EXPIRY_ENABLED, false, NodeScope, Dynamic));
            put(KNN_CACHE_ITEM_EXPIRY_TIME_MINUTES, Setting.positiveTimeSetting(KNN_CACHE_ITEM_EXPIRY_TIME_MINUTES,
                    TimeValue.timeValueHours(3), NodeScope, Dynamic));
        }
    };

    /** Latest setting value for each registered key. Thread-safe is required. */
    private final Map<String, Object> latestSettings = new ConcurrentHashMap<>();

    private ClusterService clusterService;
    private Client client;

    private KNNSettings() {}

    public static synchronized KNNSettings state() {
        if (INSTANCE == null) {
            INSTANCE = new KNNSettings();
        }
        return INSTANCE;
    }

    public void setSettingsUpdateConsumers() {
        for (Setting<?> setting : dynamicCacheSettings.values()) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(
                    setting,
                    newVal -> {
                        logger.debug("The value of setting [{}] changed to [{}]", setting.getKey(), newVal);
                        latestSettings.put(setting.getKey(), newVal);
                        // spawn a thread
                        KNNWeight.knnIndexCache.rebuild();
                    });
        }

        /**
         * We do not have to rebuild the cache for below settings
         */
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
                KNN_CIRCUIT_BREAKER_TRIGGERED_SETTING,
                newVal -> {
                    latestSettings.put(KNN_CIRCUIT_BREAKER_TRIGGERED, newVal);
                }
        );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
                KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE_SETTING,
                newVal -> {
                    latestSettings.put(KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE, newVal);
                }
        );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(
                KNN_ALGO_PARAM_INDEX_THREAD_QTY_SETTING,
                newVal -> {
                    latestSettings.put(KNN_ALGO_PARAM_INDEX_THREAD_QTY, newVal);
                }
        );
    }

    /**
     * Get setting value by key. Return default value if not configured explicitly.
     *
     * @param key   setting key.
     * @param <T> Setting type
     * @return T     setting value or default
     */
    @SuppressWarnings("unchecked")
    public <T> T getSettingValue(String key) {
        return (T) latestSettings.getOrDefault(key, getSetting(key).getDefault(Settings.EMPTY));
    }

    public Setting<?> getSetting(String key) {
        if (dynamicCacheSettings.containsKey(key)) {
            return dynamicCacheSettings.get(key);
        }

        if (KNN_CIRCUIT_BREAKER_TRIGGERED.equals(key)) {
            return KNN_CIRCUIT_BREAKER_TRIGGERED_SETTING;
        }

        if (KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE.equals(key)) {
            return KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE_SETTING;
        }

        if (KNN_ALGO_PARAM_INDEX_THREAD_QTY.equals(key)) {
            return KNN_ALGO_PARAM_INDEX_THREAD_QTY_SETTING;
        }

        throw new IllegalArgumentException("Cannot find setting by key [" + key + "]");
    }

    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings =  Arrays.asList(INDEX_KNN_SPACE_TYPE,
                INDEX_KNN_ALGO_PARAM_M_SETTING,
                INDEX_KNN_ALGO_PARAM_EF_CONSTRUCTION_SETTING,
                INDEX_KNN_ALGO_PARAM_EF_SEARCH_SETTING,
                KNN_ALGO_PARAM_INDEX_THREAD_QTY_SETTING,
                KNN_CIRCUIT_BREAKER_TRIGGERED_SETTING,
                KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE_SETTING,
                IS_KNN_INDEX_SETTING);
        return Stream.concat(settings.stream(), dynamicCacheSettings.values().stream())
                     .collect(Collectors.toList());
    }

    public static boolean isKNNPluginEnabled() {
        return KNNSettings.state().getSettingValue(KNNSettings.KNN_PLUGIN_ENABLED);
    }

    public static boolean isCircuitBreakerTriggered() {
        return KNNSettings.state().getSettingValue(KNNSettings.KNN_CIRCUIT_BREAKER_TRIGGERED);
    }

    public static ByteSizeValue getCircuitBreakerLimit() {
        return KNNSettings.state().getSettingValue(KNNSettings.KNN_MEMORY_CIRCUIT_BREAKER_LIMIT);
    }

    public static double getCircuitBreakerUnsetPercentage() {
        return KNNSettings.state().getSettingValue(KNNSettings.KNN_CIRCUIT_BREAKER_UNSET_PERCENTAGE);
    }

    public void initialize(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        setSettingsUpdateConsumers();
    }

    /**
     * Creates a setting which specifies a circuit breaker memory limit. This can either be
     * specified as an absolute bytes value or as a percentage.
     *
     * @param key the key for the setting
     * @param defaultValue the default value for this setting
     * @param properties properties properties for this setting like scope, filtering...
     * @return the setting object
     */
    public static Setting<ByteSizeValue> knnMemoryCircuitBreakerSetting(String key, String defaultValue, Setting.Property... properties) {
        return new Setting<>(key, defaultValue, (s) -> parseknnMemoryCircuitBreakerValue(s, key), properties);
    }

    public static ByteSizeValue parseknnMemoryCircuitBreakerValue(String sValue, String settingName) {
        settingName = Objects.requireNonNull(settingName);
        if (sValue != null && sValue.endsWith("%")) {
            final String percentAsString = sValue.substring(0, sValue.length() - 1);
            try {
                final double percent = Double.parseDouble(percentAsString);
                if (percent < 0 || percent > 100) {
                    throw new ElasticsearchParseException("percentage should be in [0-100], got [{}]", percentAsString);
                }
                long physicalMemoryInBytes = osProbe.getTotalPhysicalMemorySize();
                if (physicalMemoryInBytes <= 0) {
                    throw new IllegalStateException("Physical memory size could not be determined");
                }
                long esJvmSizeInBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
                long eligibleMemoryInBytes = physicalMemoryInBytes - esJvmSizeInBytes;
                return new ByteSizeValue((long) ((percent / 100) * eligibleMemoryInBytes), ByteSizeUnit.BYTES);
            } catch (NumberFormatException e) {
                throw new ElasticsearchParseException("failed to parse [{}] as a double", e, percentAsString);
            }
        } else {
            return parseBytesSizeValue(sValue, settingName);
        }
    }

    /**
     * Updates knn.circuit_breaker.triggered setting to true/false
     * @param flag true/false
     */
    public synchronized void updateCircuitBreakerSettings(boolean flag) {
        ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest();
        Settings circuitBreakerSettings = Settings.builder()
                                                  .put(KNNSettings.KNN_CIRCUIT_BREAKER_TRIGGERED, flag)
                                                  .build();
        clusterUpdateSettingsRequest.persistentSettings(circuitBreakerSettings);
        client.admin().cluster().updateSettings(clusterUpdateSettingsRequest,
                new ActionListener<ClusterUpdateSettingsResponse>() {
            @Override
            public void onResponse(ClusterUpdateSettingsResponse clusterUpdateSettingsResponse) {
                logger.debug("Cluster setting {}, acknowledged: {} ",
                        clusterUpdateSettingsRequest.persistentSettings(),
                        clusterUpdateSettingsResponse.isAcknowledged());
            }
            @Override
            public void onFailure(Exception e) {
                logger.info("Exception while updating circuit breaker setting {} to {}",
                        clusterUpdateSettingsRequest.persistentSettings(), e.getMessage());
            }
        });
    }

    /**
     *
     * @param index Name of the index
     * @return efSearch value
     */
    public static int getEfSearchParam(String index) {
        return getIndexSettingValue(index, KNN_ALGO_PARAM_EF_SEARCH, 512);
    }

    /**
     *
     * @param index Name of the index
     * @return spaceType value
     */
    public static String getSpaceType(String index) {
        return KNNSettings.state().clusterService.state().getMetadata()
            .index(index).getSettings().get(KNN_SPACE_TYPE, SpaceTypes.l2.getValue());
    }

    public static int getIndexSettingValue(String index, String settingName, int defaultValue) {
        return KNNSettings.state().clusterService.state().getMetadata()
                                                 .index(index).getSettings()
                                                 .getAsInt(settingName, defaultValue);
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    static class SpaceTypeValidator implements Setting.Validator<String> {

        private Set<String> types = SpaceTypes.getValues();

        @Override public void validate(String value) {
            if (value == null || !types.contains(value.toLowerCase())){
                throw new InvalidParameterException(String.format("Unsupported space type: %s", value));
            }
        }
    }

    public void onIndexModule(IndexModule module) {
        module.addSettingsUpdateConsumer(
                INDEX_KNN_ALGO_PARAM_EF_SEARCH_SETTING,
                newVal -> {
                    logger.debug("The value of [KNN] setting [{}] changed to [{}]", KNN_ALGO_PARAM_EF_SEARCH, newVal);
                    latestSettings.put(KNN_ALGO_PARAM_EF_SEARCH, newVal);
                    // TODO: replace cache-rebuild with index reload into the cache
                    KNNWeight.knnIndexCache.rebuild();
                });
    }
}
