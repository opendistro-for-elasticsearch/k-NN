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

#include "com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFaissIndex.h"
#include "jni_util.h"

#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

#include "faiss/index_factory.h"
#include "faiss/MetaIndexes.h"
#include "faiss/index_io.h"
#include "faiss/IndexHNSW.h"


using std::string;
using std::vector;

// mapMetric is used to map a string from the plugin to a faiss metric. All translation should be done via this map
std::unordered_map<string, faiss::MetricType> mapMetric = {
	{"l2", faiss::METRIC_L2},
	{"innerproduct", faiss::METRIC_INNER_PRODUCT}
};


/**
 * Method: saveIndex
 *
 */
	JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFaissIndex_saveIndex
(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath, jobjectArray algoParams, jstring spaceType)
{
	vector<int64_t> idVector;
	vector<float>   dataset;
	vector<string> paramsList;
	//TODO we can support other FAISS index in the future, may be paramsList can add index=xxxx
	string indexDescription = "HNSW32";
	faiss::MetricType metric = faiss::METRIC_L2;
	std::unique_ptr<faiss::Index> indexWriter;
	int dim = 0;
	try {
		//---- ids
		int* object_ids = nullptr;
		object_ids = env->GetIntArrayElements(ids, 0);
		for(int i = 0; i < env->GetArrayLength(ids); ++i) {
			idVector.push_back(object_ids[i]);
		}
		env->ReleaseIntArrayElements(ids, object_ids, 0);
		knn_jni::HasExceptionInStack(env);

		//---- vectors
		for (int i = 0; i < env->GetArrayLength(vectors); ++i) {
			auto vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
			float* vector = env->GetFloatArrayElements(vectorArray, 0);
			dim = env->GetArrayLength(vectorArray);
			for(int j = 0; j < dim; ++j) {
				dataset.push_back(vector[j]);
			}
			env->ReleaseFloatArrayElements(vectorArray, vector, 0);
		}
        knn_jni::HasExceptionInStack(env);

		//---- indexPath
		const char *indexString = env->GetStringUTFChars(indexPath, 0);
		string indexPathString(indexString);
		env->ReleaseStringUTFChars(indexPath, indexString);
        knn_jni::HasExceptionInStack(env);

		//---- algoParams
		int paramsCount = env->GetArrayLength(algoParams);
		for (int i=0; i<paramsCount; i++) {
			jstring param = (jstring) (env->GetObjectArrayElement(algoParams, i));
			const char *rawString = env->GetStringUTFChars(param, 0);
			paramsList.push_back(rawString);

			int M = 32;
			if (sscanf(rawString, "M=%d", &M) == 1) {
				indexDescription="HNSW"+std::to_string(M);
			}
			env->ReleaseStringUTFChars(param, rawString);

		}
        knn_jni::HasExceptionInStack(env);


		//---- space
		const char *spaceTypeCStr = env->GetStringUTFChars(spaceType, 0);
		string spaceTypeString(spaceTypeCStr);
		env->ReleaseStringUTFChars(spaceType, spaceTypeCStr);
        knn_jni::HasExceptionInStack(env);
		// space mapping faiss::MetricType
		if(mapMetric.find(spaceTypeString) != mapMetric.end()) {
			metric = mapMetric[spaceTypeString];
		}

		//---- Create IndexWriter from faiss index_factory
		indexWriter.reset(faiss::index_factory(dim, indexDescription.data(), metric));

		//Preparation And TODO Verify IndexWriter
		//Some Param Can not Create from IndexFactory, Like HNSW efSearch and efCOnstruction
		//----FOR HNSW 1st PARAM: M(HNSW32->M=32), efConstruction, efSearch
		if(indexDescription.find("HNSW") != std::string::npos) {
			for(int i = 0; i < paramsCount; ++i) {
				const string& param = paramsList[i];
				int efConstruction = 40; //default
				int efSearch = 16;//default
				if(param.find("efConstruction") != std::string::npos &&
						sscanf(param.data(), "efConstruction=%d", &efConstruction) == 1) {
					faiss::IndexHNSW* ihp = reinterpret_cast<faiss::IndexHNSW*>(indexWriter.get());
					ihp->hnsw.efConstruction = efConstruction;
				} else if (param.find("efSearch") != std::string::npos &&
						sscanf(param.data(), "efSearch=%d", &efSearch) == 1){
					faiss::IndexHNSW* ihp = reinterpret_cast<faiss::IndexHNSW*>(indexWriter.get());
					ihp->hnsw.efSearch = efSearch;
				}
			}
		}

		//---- Do Index
		//----- 1. Train
		if(!indexWriter->is_trained) {
			//TODO if we use like PQ, we have to train dataset
			// but when a lucene segment only one document, it
			// can not train the data.
		}
		//----- 2. Add IDMap
		// default all use self defined IndexIDMap cause some class no add_with_ids
		faiss::IndexIDMap idMap =  faiss::IndexIDMap(indexWriter.get());
		idMap.add_with_ids(idVector.size(), dataset.data(), idVector.data());

		//----- 3. WriteIndex
		faiss::write_index(&idMap, indexPathString.c_str());

		//Explicit delete object
		faiss::Index* indexPointer = indexWriter.release();
		if(indexPointer) delete indexPointer;

	}
	catch(...) {
		faiss::Index* indexPointer = indexWriter.release();
		if(indexPointer) delete indexPointer;
        knn_jni::CatchCppExceptionAndThrowJava(env);
	}
}


	JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFaissIndex_queryIndex
(JNIEnv* env, jclass cls, jlong indexPointer, jfloatArray queryVector, jint k)
{
	faiss::Index *indexReader = nullptr;
	try {
		indexReader = reinterpret_cast<faiss::Index*>(indexPointer);
		float* rawQueryvector = env->GetFloatArrayElements(queryVector, 0);
		int dim	= env->GetArrayLength(queryVector);

		std::vector<float> dis(k * dim);
		std::vector<faiss::Index::idx_t> ids( k * dim);
		indexReader->search(1, rawQueryvector, k, dis.data(), ids.data());
		env->ReleaseFloatArrayElements(queryVector, rawQueryvector, 0);
        knn_jni::HasExceptionInStack(env);

		int resultSize = k;
		//if result is not enough, padded with -1s
		for(int i = 0; i < k; i++) {
			if(ids[i] == -1) {
				resultSize = i;
				break;
			}
		}

		jclass resultClass = env->FindClass("com/amazon/opendistroforelasticsearch/knn/index/KNNQueryResult");
		jmethodID allArgs = env->GetMethodID(resultClass, "<init>", "(IF)V");
		jobjectArray results = env->NewObjectArray(resultSize, resultClass, NULL);
		for(int i = 0; i < resultSize; ++i) {
			float distance = dis[i];
			long  id = ids[i];
			env->SetObjectArrayElement(results, i, env->NewObject(resultClass, allArgs, id, distance));
		}
        knn_jni::HasExceptionInStack(env);
		return results;

	}
	catch(...) {
		if(indexReader) delete indexReader;
        knn_jni::CatchCppExceptionAndThrowJava(env);
	}
	return NULL;
}

	JNIEXPORT jlong JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFaissIndex_init
(JNIEnv* env, jclass cls,  jstring indexPath, jobjectArray algoParams, jstring spaceType)
{

	faiss::Index* indexReader = nullptr;
	try {
		const char *indexPathCStr = env->GetStringUTFChars(indexPath, 0);
		string indexPathString(indexPathCStr);
		env->ReleaseStringUTFChars(indexPath, indexPathCStr);
        knn_jni::HasExceptionInStack(env);
		//whether set IO_FLAGS = 0 or IO_FLAG_READ_ONLYfaiss::IO_FLAG_READ_ONLY
		indexReader = faiss::read_index(indexPathString.c_str(), faiss::IO_FLAG_READ_ONLY);
		return (jlong) indexReader;
	} 
	catch(...) {
		if (indexReader) delete indexReader;
        knn_jni::CatchCppExceptionAndThrowJava(env);
	}
	return NULL;
}

/**
 * When autoclose class do close, then delete the pointer
 * Method GC pointer
 */
	JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFaissIndex_gc
(JNIEnv* env, jclass cls,  jlong indexPointer)
{
	try {
		faiss::Index *indexWrapper = reinterpret_cast<faiss::Index*>(indexPointer);
        knn_jni::HasExceptionInStack(env);
		delete indexWrapper;
        knn_jni::HasExceptionInStack(env);
	}
	catch (...) {
        knn_jni::CatchCppExceptionAndThrowJava(env);
	}
}

/**
 * Method: Global Init
 *
 */
JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFaissIndex_initLibrary(JNIEnv *, jclass)
{
	//set thread 1 cause ES has Search thread
	//TODO make it different at search and write
	//	omp_set_num_threads(1);
}
