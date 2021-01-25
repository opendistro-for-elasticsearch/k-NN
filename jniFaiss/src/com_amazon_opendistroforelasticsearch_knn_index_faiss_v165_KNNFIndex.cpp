#include "com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFIndex.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <sys/time.h>
#include <omp.h>

#include "faiss/index_factory.h"
#include "faiss/MetaIndexes.h"
#include "faiss/index_io.h"
#include "faiss/IndexHNSW.h"


using std::string;
using std::vector;

std::unordered_map<string, faiss::MetricType> mapMetric = {
	{"l2", faiss::METRIC_L2},
	{"innerproduct", faiss::METRIC_INNER_PRODUCT}
};

extern "C"

struct JavaException {
	JavaException(JNIEnv* env, const char* type = "", const char* message = "")
	{
		jclass newExcCls = env->FindClass(type);
		if (newExcCls != NULL)
			env->ThrowNew(newExcCls, message);
	}
};

inline void has_exception_in_stack(JNIEnv* env)
{
	if (env->ExceptionCheck() == JNI_TRUE)
		throw std::runtime_error("Exception Occured");
}

void catch_cpp_exception_and_throw_java(JNIEnv* env)
{
	try {
		throw;
	}
	catch (const std::bad_alloc& rhs) {
		JavaException(env, "java/io/IOException", rhs.what());
	}
	catch (const std::runtime_error& re) {
		JavaException(env, "java/lang/Exception", re.what());
	}
	catch (const std::exception& e) {
		JavaException(env, "java/lang/Exception", e.what());
	}
	catch (...) {
		JavaException(env, "java/lang/Exception", "Unknown exception occured");
	}
}

/**
 * Method: saveIndex
 *
 */
	JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFIndex_saveIndex
(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath, jobjectArray algoParams, jstring spaceType)
{
	vector<int64_t> idVector;
	vector<float>   dataset;
	vector<string> paramsList;
	string indexDescription = "HNSW32";
	faiss::MetricType metric = faiss::METRIC_L2;
	std::unique_ptr<faiss::Index> indexWriter;
	int dim = 0;
	try {
		//---- ids
		int* object_ids = NULL;
		object_ids = env->GetIntArrayElements(ids, 0);
		for(int i = 0; i < env->GetArrayLength(ids); ++i) {
			idVector.push_back(object_ids[i]);
		}
		env->ReleaseIntArrayElements(ids, object_ids, 0);
		has_exception_in_stack(env);

		//---- vectors
		for (int i = 0; i < env->GetArrayLength(vectors); ++i) {
			jfloatArray vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
			float* vector = env->GetFloatArrayElements(vectorArray, 0);
			dim = env->GetArrayLength(vectorArray);
			for(int j = 0; j < dim; ++j) {
				dataset.push_back(vector[j]);
			}
			env->ReleaseFloatArrayElements(vectorArray, vector, 0);
		}
		has_exception_in_stack(env);

		//---- indexPath
		const char *indexString = env->GetStringUTFChars(indexPath, 0);
		string indexPathString(indexString);
		env->ReleaseStringUTFChars(indexPath, indexString);
		has_exception_in_stack(env);

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
		has_exception_in_stack(env);


		//---- space
		const char *spaceTypeCStr = env->GetStringUTFChars(spaceType, 0);
		string spaceTypeString(spaceTypeCStr);
		env->ReleaseStringUTFChars(spaceType, spaceTypeCStr);
		has_exception_in_stack(env);
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
		catch_cpp_exception_and_throw_java(env);
	}
}


	JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFIndex_queryIndex
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
		has_exception_in_stack(env);

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
		has_exception_in_stack(env);
		return results;

	}
	catch(...) {
		if(indexReader) delete indexReader;
		catch_cpp_exception_and_throw_java(env);	
	}
	return NULL;
}

	JNIEXPORT jlong JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFIndex_init
(JNIEnv* env, jclass cls,  jstring indexPath, jobjectArray algoParams, jstring spaceType)
{

	faiss::Index* indexReader = nullptr;
	try {
		const char *indexPathCStr = env->GetStringUTFChars(indexPath, 0);
		string indexPathString(indexPathCStr);
		env->ReleaseStringUTFChars(indexPath, indexPathCStr);
		has_exception_in_stack(env);
		//whether set IO_FLAGS = 0 or IO_FLAG_READ_ONLYfaiss::IO_FLAG_READ_ONLY
		indexReader = faiss::read_index(indexPathString.c_str(), faiss::IO_FLAG_READ_ONLY);
		return (jlong) indexReader;
	} 
	catch(...) {
		if (indexReader) delete indexReader;
		catch_cpp_exception_and_throw_java(env);	
	}
	return NULL;
}

/**
 * When autoclose class do close, then delete the pointer
 * Method GC pointer
 */
	JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFIndex_gc
(JNIEnv* env, jclass cls,  jlong indexPointer)
{
	try {
		faiss::Index *indexWrapper = reinterpret_cast<faiss::Index*>(indexPointer);
		has_exception_in_stack(env);
		delete indexWrapper;
		has_exception_in_stack(env);
	}
	catch (...) {
		catch_cpp_exception_and_throw_java(env);
	}
}

/**
 * Method: Global Init
 *
 */
JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFIndex_initLibrary(JNIEnv *, jclass)
{
	//set thread 1 cause ES has Search thread
	//TODO make it different at search and write
	//	omp_set_num_threads(1);
}
