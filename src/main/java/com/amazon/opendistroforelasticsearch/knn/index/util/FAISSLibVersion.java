package com.amazon.opendistroforelasticsearch.knn.index.util;

public enum FAISSLibVersion {

    /**
     * Latest available faiss version
     */
    VFAISS_165("FAISS_165") {
        @Override
        public String indexLibraryVersion() {
            return "KNNIndex_FAISS_V1_6_5";
        }
    };

    public static final FAISSLibVersion LATEST = VFAISS_165;

    public String buildVersion;

    FAISSLibVersion(String buildVersion) {
        this.buildVersion = buildVersion;
    }

    /**
     * FAISS library version used by the KNN codec
     * @return name
     */
    public abstract String indexLibraryVersion();

    public String getBuildVersion() { return buildVersion; }
}
