package com.amazon.opendistroforelasticsearch.knn.index.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum KNNEngine {

    NMSLIB("NMSLIB") {
        @Override
        public String getLatestBuildVersion() {
            return NmsLibVersion.LATEST.buildVersion;
        }

        @Override
        public String getLatestLibVersion() {
            return NmsLibVersion.LATEST.indexLibraryVersion();
        }
    },
    FAISS("FAISS") {
        @Override
        public String getLatestBuildVersion() {
            return FAISSLibVersion.LATEST.buildVersion;
        }

        @Override
        public String getLatestLibVersion() {
            return FAISSLibVersion.LATEST.indexLibraryVersion();
        }
    };
    public static final KNNEngine DEFAULT = FAISS;

    KNNEngine(String knnEngineName) {
        this.knnEngineName = knnEngineName;
    }

    public String knnEngineName;

    public abstract String getLatestBuildVersion();
    public abstract String getLatestLibVersion();

    public String getKnnEngineName() {
        return knnEngineName;
    }

    public static Set<String> getEngines() {
        Set<String> values = new HashSet<>();
        for (KNNEngine engineName : KNNEngine.values()) {
            values.add(engineName.getKnnEngineName());
        }
        return values;
    }

    public static KNNEngine getEngine(String name) {
        if(FAISS.knnEngineName.contains(name)) {
            return FAISS;
        } else {
            return NMSLIB;
        }
    }
}
