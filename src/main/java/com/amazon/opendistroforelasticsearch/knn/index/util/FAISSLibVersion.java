package com.amazon.opendistroforelasticsearch.knn.index.util;

public enum FAISSLibVersion {

    /**
     * Latest available faiss version
     */
    VFAISS_164("FAISS_164") {
        @Override
        public String indexLibraryVersion() {
            return "KNNIndex_FAISS_V1_6_4";
        }
    };

    public static final FAISSLibVersion LATEST = VFAISS_164;

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
