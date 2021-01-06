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

#include "com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex.h"

#include "init.h"
#include "index.h"
#include "params.h"
#include "knnquery.h"
#include "knnqueue.h"
#include "methodfactory.h"
#include "spacefactory.h"
#include "space.h"
#include "unordered_set"

using std::vector;

using similarity::initLibrary;
using similarity::AnyParams;
using similarity::Index;
using similarity::MethodFactoryRegistry;
using similarity::SpaceFactoryRegistry;
using similarity::AnyParams;
using similarity::Space;
using similarity::ObjectVector;
using similarity::Object;
using similarity::KNNQuery;
using similarity::KNNQueue;

extern "C"

const char* datSuffix = ".dat";

std::unordered_set<string> OptimizedSpace {"l2", "cosinesimil"};
std::unordered_set<string> IntSpace { "bit_hamming" };
template <typename dist_t>
struct IndexWrapper {
  IndexWrapper(string spaceType) {
    space.reset(SpaceFactoryRegistry<dist_t>::Instance().CreateSpace(spaceType, AnyParams()));
    index.reset(MethodFactoryRegistry<dist_t>::Instance().CreateMethod(false, "hnsw", spaceType, *space, data));
  }
  std::unique_ptr<Space<dist_t>> space;
  std::unique_ptr<Index<dist_t>> index;
  // Index gets constructed with a reference to data (see above) but is otherwise unused
  ObjectVector data;
};

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

void freeAndClearObjectVector(ObjectVector& data) {
    for (auto datum : data) {
        delete datum;
    }
    data.clear();
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_saveIndex(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath, jobjectArray algoParams, jstring spaceType, jboolean loadData)
{
    Space<float>* space = NULL;
    ObjectVector dataset;
    Index<float>* index = NULL;
    int* object_ids = NULL;

    try {
        const char *spaceTypeCStr = env->GetStringUTFChars(spaceType, 0);
        string spaceTypeString(spaceTypeCStr);
        env->ReleaseStringUTFChars(spaceType, spaceTypeCStr);
        has_exception_in_stack(env);
        space = SpaceFactoryRegistry<float>::Instance().CreateSpace(spaceTypeString, AnyParams());
        object_ids = env->GetIntArrayElements(ids, 0);
        for (int i = 0; i < env->GetArrayLength(vectors); i++) {
            jfloatArray vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
            float* vector = env->GetFloatArrayElements(vectorArray, 0);
            dataset.push_back(new Object(object_ids[i], -1, env->GetArrayLength(vectorArray)*sizeof(float), vector));
            env->ReleaseFloatArrayElements(vectorArray, vector, 0);
        }
        // free up memory
        env->ReleaseIntArrayElements(ids, object_ids, 0);
        index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", spaceTypeString, *space, dataset);

        int paramsCount = env->GetArrayLength(algoParams);
        vector<string> paramsList;
        for (int i=0; i<paramsCount; i++) {
            jstring param = (jstring) (env->GetObjectArrayElement(algoParams, i));
            const char *rawString = env->GetStringUTFChars(param, 0);
            paramsList.push_back(rawString);
            env->ReleaseStringUTFChars(param, rawString);
        }

        index->CreateIndex(AnyParams(paramsList));
        has_exception_in_stack(env);
        const char *indexString = env->GetStringUTFChars(indexPath, 0);
        // Write object vector binary data for spaces not supporting optimized index
        if (OptimizedSpace.find(spaceTypeString) == OptimizedSpace.end() || loadData){
            string indexPathString(indexString);
            vector<string> dummy;
            space->WriteObjectVectorBinData(dataset, dummy, indexPathString + datSuffix);
        }
        index->SaveIndex(indexString);
        env->ReleaseStringUTFChars(indexPath, indexString);
        has_exception_in_stack(env);

        // Free each object in the dataset. No need to clear the vector because it goes out of scope
        // immediately
        for (auto it = dataset.begin(); it != dataset.end(); it++) {
             delete *it;
        }
        delete index;
        delete space;
    }
    catch (...) {
        if (object_ids) { env->ReleaseIntArrayElements(ids, object_ids, 0); }
        for (auto it = dataset.begin(); it != dataset.end(); it++) {
             delete *it;
        }
        if (index) { delete index; }
        if (space) { delete space; }
        catch_cpp_exception_and_throw_java(env);
    }
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_saveIndexI(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath, jobjectArray algoParams, jstring spaceType, jboolean loadData)
{
    Space<int>* space = NULL;
    ObjectVector dataset;
    Index<int>* index = NULL;
    int* object_ids = NULL;

    try {
        const char *spaceTypeCStr = env->GetStringUTFChars(spaceType, 0);
        string spaceTypeString(spaceTypeCStr);
        env->ReleaseStringUTFChars(spaceType, spaceTypeCStr);
        has_exception_in_stack(env);
        space = SpaceFactoryRegistry<int>::Instance().CreateSpace(spaceTypeString, AnyParams());
        object_ids = env->GetIntArrayElements(ids, 0);
        for (int i = 0; i < env->GetArrayLength(vectors); i++) {
            jintArray vectorArray = (jintArray)env->GetObjectArrayElement(vectors, i);
            int* vector = env->GetIntArrayElements(vectorArray, 0);
			int jintArrayLen = env->GetArrayLength(vectorArray);
			std::vector<int> vecWithSize(vector, vector + jintArrayLen);
            env->ReleaseIntArrayElements(vectorArray, vector, 0);

			//As space_bit_vector.h shows, we need Put the number of elements in the end
			vecWithSize.push_back(vecWithSize.size());	

            dataset.push_back(new Object(object_ids[i], -1, vecWithSize.size()*sizeof(int), vecWithSize.data()));
        }
        // free up memory
        env->ReleaseIntArrayElements(ids, object_ids, 0);
        index = MethodFactoryRegistry<int>::Instance().CreateMethod(false, "hnsw", spaceTypeString, *space, dataset);

        int paramsCount = env->GetArrayLength(algoParams);
        vector<string> paramsList;
        for (int i=0; i<paramsCount; i++) {
            jstring param = (jstring) (env->GetObjectArrayElement(algoParams, i));
            const char *rawString = env->GetStringUTFChars(param, 0);
            paramsList.push_back(rawString);
            env->ReleaseStringUTFChars(param, rawString);
        }

        index->CreateIndex(AnyParams(paramsList));
        has_exception_in_stack(env);
        const char *indexString = env->GetStringUTFChars(indexPath, 0);
        // Write object vector binary data for spaces not supporting optimized index
        if (OptimizedSpace.find(spaceTypeString) == OptimizedSpace.end() || loadData){
            string indexPathString(indexString);
            vector<string> dummy;
            space->WriteObjectVectorBinData(dataset, dummy, indexPathString + datSuffix);
        }
        index->SaveIndex(indexString);
        env->ReleaseStringUTFChars(indexPath, indexString);
        has_exception_in_stack(env);

        // Free each object in the dataset. No need to clear the vector because it goes out of scope
        // immediately
        for (auto it = dataset.begin(); it != dataset.end(); it++) {
             delete *it;
        }
        delete index;
        delete space;
    }
    catch (...) {
        if (object_ids) { env->ReleaseIntArrayElements(ids, object_ids, 0); }
        for (auto it = dataset.begin(); it != dataset.end(); it++) {
             delete *it;
        }
        if (index) { delete index; }
        if (space) { delete space; }
        catch_cpp_exception_and_throw_java(env);
    }
}
JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_saveIndexB(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath, jobjectArray algoParams, jstring spaceType, jboolean loadData)
{
    Space<int>* space = NULL;
    ObjectVector dataset;
    Index<int>* index = NULL;
    int* object_ids = NULL;

    try {
        const char *spaceTypeCStr = env->GetStringUTFChars(spaceType, 0);
        string spaceTypeString(spaceTypeCStr);
        env->ReleaseStringUTFChars(spaceType, spaceTypeCStr);
        has_exception_in_stack(env);
        space = SpaceFactoryRegistry<int>::Instance().CreateSpace(spaceTypeString, AnyParams());
        object_ids = env->GetIntArrayElements(ids, 0);
        for (int i = 0; i < env->GetArrayLength(vectors); i++) {
            jstring vectorArray = (jstring)env->GetObjectArrayElement(vectors, i);
            const char *vector = env->GetStringUTFChars(vectorArray, 0);
            string vectrStr(vector);
            dataset.push_back(
                space->CreateObjFromStr(object_ids[i], -1, vectrStr, NULL).release()
            );
            env->ReleaseStringUTFChars(vectorArray, vector);
        }
        // free up memory
        env->ReleaseIntArrayElements(ids, object_ids, 0);
        index = MethodFactoryRegistry<int>::Instance().CreateMethod(false, "hnsw", spaceTypeString, *space, dataset);

        int paramsCount = env->GetArrayLength(algoParams);
        vector<string> paramsList;
        for (int i=0; i<paramsCount; i++) {
            jstring param = (jstring) (env->GetObjectArrayElement(algoParams, i));
            const char *rawString = env->GetStringUTFChars(param, 0);
            paramsList.push_back(rawString);
            env->ReleaseStringUTFChars(param, rawString);
        }

        index->CreateIndex(AnyParams(paramsList));
        has_exception_in_stack(env);
        const char *indexString = env->GetStringUTFChars(indexPath, 0);
        // Write object vector binary data for spaces not supporting optimized index
        if (OptimizedSpace.find(spaceTypeString) == OptimizedSpace.end() || loadData){
            string indexPathString(indexString);
            vector<string> dummy;
            space->WriteObjectVectorBinData(dataset, dummy, indexPathString + datSuffix);
        }
        index->SaveIndex(indexString);
        env->ReleaseStringUTFChars(indexPath, indexString);
        has_exception_in_stack(env);

        // Free each object in the dataset. No need to clear the vector because it goes out of scope
        // immediately
        for (auto it = dataset.begin(); it != dataset.end(); it++) {
             delete *it;
        }
        delete index;
        delete space;
    }
    catch (...) {
        if (object_ids) { env->ReleaseIntArrayElements(ids, object_ids, 0); }
        for (auto it = dataset.begin(); it != dataset.end(); it++) {
             delete *it;
        }
        if (index) { delete index; }
        if (space) { delete space; }
        catch_cpp_exception_and_throw_java(env);
    }
}
JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_queryIndex(JNIEnv* env, jclass cls, jlong indexPointer, jfloatArray queryVector, jint k)
{
    try {
        IndexWrapper<float> *indexWrapper = reinterpret_cast<IndexWrapper<float>*>(indexPointer);

        float* rawQueryvector = env->GetFloatArrayElements(queryVector, 0);
        std::unique_ptr<const Object> queryObject(new Object(-1, -1, env->GetArrayLength(queryVector)*sizeof(float), rawQueryvector));
        env->ReleaseFloatArrayElements(queryVector, rawQueryvector, 0);
        has_exception_in_stack(env);

        KNNQuery<float> knnQuery(*(indexWrapper->space), queryObject.get(), k);
        indexWrapper->index->Search(&knnQuery);
        std::unique_ptr<KNNQueue<float>> result(knnQuery.Result()->Clone());
        has_exception_in_stack(env);
        int resultSize = result->Size();
        jclass resultClass = env->FindClass("com/amazon/opendistroforelasticsearch/knn/index/KNNQueryResult");
        jmethodID allArgs = env->GetMethodID(resultClass, "<init>", "(IF)V");
        jobjectArray results = env->NewObjectArray(resultSize, resultClass, NULL);
        for (int i = 0; i < resultSize; i++) {
            float distance = result->TopDistance();
            long id = result->Pop()->id();
            env->SetObjectArrayElement(results, i, env->NewObject(resultClass, allArgs, id, distance));
        }
        has_exception_in_stack(env);
        return results;
    } catch(...) {
        catch_cpp_exception_and_throw_java(env);
    }
    return NULL;
}
JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_queryIndexI(JNIEnv* env, jclass cls, jlong indexPointer, jintArray queryVector, jint k)
{
    try {
        IndexWrapper<int> *indexWrapper = reinterpret_cast<IndexWrapper<int>*>(indexPointer);

        int* rawQueryvector = env->GetIntArrayElements(queryVector, 0);
		int  jintArrayLen = env->GetArrayLength(queryVector);
		vector<int> vecRawQueryVector(rawQueryvector, rawQueryvector+jintArrayLen );

		//As space_bit_vector.h shows, we need Put the number of elements in the end
		vecRawQueryVector.push_back(vecRawQueryVector.size());

        std::unique_ptr<const Object> queryObject(new Object(-1, -1, vecRawQueryVector.size()*sizeof(int), vecRawQueryVector.data()));
        env->ReleaseIntArrayElements(queryVector, rawQueryvector, 0);
        has_exception_in_stack(env);

        KNNQuery<int> knnQuery(*(indexWrapper->space), queryObject.get(), k);
        indexWrapper->index->Search(&knnQuery);
        std::unique_ptr<KNNQueue<int>> result(knnQuery.Result()->Clone());
        has_exception_in_stack(env);
        int resultSize = result->Size();
        jclass resultClass = env->FindClass("com/amazon/opendistroforelasticsearch/knn/index/KNNQueryResult");
        jmethodID allArgs = env->GetMethodID(resultClass, "<init>", "(IF)V");
        jobjectArray results = env->NewObjectArray(resultSize, resultClass, NULL);
        for (int i = 0; i < resultSize; i++) {
            float distance = result->TopDistance();
            long id = result->Pop()->id();
            env->SetObjectArrayElement(results, i, env->NewObject(resultClass, allArgs, id, distance));
        }
        has_exception_in_stack(env);
        return results;
    } catch(...) {
        catch_cpp_exception_and_throw_java(env);
    }
    return NULL;
}
JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_queryIndexB(JNIEnv* env, jclass cls, jlong indexPointer, jstring queryVector, jint k)
{
    try {
        IndexWrapper<int> *indexWrapper = reinterpret_cast<IndexWrapper<int>*>(indexPointer);

        const char *rawQueryvector = env->GetStringUTFChars(queryVector, 0);
        string rawQueryvectorStr(rawQueryvector);
        env->ReleaseStringUTFChars(queryVector, rawQueryvector);
        std::unique_ptr<const Object> queryObject(
            indexWrapper->space->CreateObjFromStr(-1, -1, rawQueryvectorStr, NULL).release()
        );
        has_exception_in_stack(env);

        KNNQuery<int> knnQuery(*(indexWrapper->space), queryObject.get(), k);
        indexWrapper->index->Search(&knnQuery);
        std::unique_ptr<KNNQueue<int>> result(knnQuery.Result()->Clone());
        has_exception_in_stack(env);
        int resultSize = result->Size();
        jclass resultClass = env->FindClass("com/amazon/opendistroforelasticsearch/knn/index/KNNQueryResult");
        jmethodID allArgs = env->GetMethodID(resultClass, "<init>", "(IF)V");
        jobjectArray results = env->NewObjectArray(resultSize, resultClass, NULL);
        for (int i = 0; i < resultSize; i++) {
            float distance = result->TopDistance();
            long id = result->Pop()->id();
            env->SetObjectArrayElement(results, i, env->NewObject(resultClass, allArgs, id, distance));
        }
        has_exception_in_stack(env);
        return results;
    } catch(...) {
        catch_cpp_exception_and_throw_java(env);
    }
    return NULL;
}
JNIEXPORT jlong JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_init(JNIEnv* env, jclass cls,  jstring indexPath, jobjectArray algoParams, jstring spaceType, jboolean loadData)
{
    IndexWrapper<float> *indexWrapper = NULL;
    try {
        const char *indexPathCStr = env->GetStringUTFChars(indexPath, 0);
        string indexPathString(indexPathCStr);
        env->ReleaseStringUTFChars(indexPath, indexPathCStr);
        has_exception_in_stack(env);

        // Load index from file (may throw)
        const char *spaceTypeCStr = env->GetStringUTFChars(spaceType, 0);
        string spaceTypeString(spaceTypeCStr);
        env->ReleaseStringUTFChars(spaceType, spaceTypeCStr);
        has_exception_in_stack(env);
        IndexWrapper<float> *indexWrapper = new IndexWrapper<float>(spaceTypeString);
        // Read object vector binary data for spaces not supporting optimized index
        if (OptimizedSpace.find(spaceTypeString) == OptimizedSpace.end() || loadData){
            vector<string> dummy;
            freeAndClearObjectVector(indexWrapper->data);
            indexWrapper->space->ReadObjectVectorFromBinData(indexWrapper->data, dummy, indexPathString + datSuffix);
        }
        indexWrapper->index->LoadIndex(indexPathString);

        // Parse and set query params
        int paramsCount = env->GetArrayLength(algoParams);
        vector<string> paramsList;
        for (int i=0; i<paramsCount; i++) {
            jstring param = (jstring) (env->GetObjectArrayElement(algoParams, i));
            const char *rawString = env->GetStringUTFChars(param, 0);
            paramsList.push_back(rawString);
            env->ReleaseStringUTFChars(param, rawString);
        }
        indexWrapper->index->SetQueryTimeParams(AnyParams(paramsList));
        has_exception_in_stack(env);

        return (jlong) indexWrapper;
    }
    // nmslib seems to throw std::runtime_error if the index cannot be read (which
    // is the only known failure mode for init()).
    catch (...) {
        if (indexWrapper) delete indexWrapper;
        catch_cpp_exception_and_throw_java(env);
    }
    return NULL;
}

JNIEXPORT jlong JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_initI(JNIEnv* env, jclass cls,  jstring indexPath, jobjectArray algoParams, jstring spaceType, jboolean loadData)
{
    IndexWrapper<int> *indexWrapper = NULL;
    try {
        const char *indexPathCStr = env->GetStringUTFChars(indexPath, 0);
        string indexPathString(indexPathCStr);
        env->ReleaseStringUTFChars(indexPath, indexPathCStr);
        has_exception_in_stack(env);

        // Load index from file (may throw)
        const char *spaceTypeCStr = env->GetStringUTFChars(spaceType, 0);
        string spaceTypeString(spaceTypeCStr);
        env->ReleaseStringUTFChars(spaceType, spaceTypeCStr);
        has_exception_in_stack(env);
        IndexWrapper<int> *indexWrapper = new IndexWrapper<int>(spaceTypeString);
        // Read object vector binary data for spaces not supporting optimized index
        if (OptimizedSpace.find(spaceTypeString) == OptimizedSpace.end() || loadData){
            vector<string> dummy;
            freeAndClearObjectVector(indexWrapper->data);
            indexWrapper->space->ReadObjectVectorFromBinData(indexWrapper->data, dummy, indexPathString + datSuffix);
        }
        indexWrapper->index->LoadIndex(indexPathString);

        // Parse and set query params
        int paramsCount = env->GetArrayLength(algoParams);
        vector<string> paramsList;
        for (int i=0; i<paramsCount; i++) {
            jstring param = (jstring) (env->GetObjectArrayElement(algoParams, i));
            const char *rawString = env->GetStringUTFChars(param, 0);
            paramsList.push_back(rawString);
            env->ReleaseStringUTFChars(param, rawString);
        }
        indexWrapper->index->SetQueryTimeParams(AnyParams(paramsList));
        has_exception_in_stack(env);

        return (jlong) indexWrapper;
    }
    // nmslib seems to throw std::runtime_error if the index cannot be read (which
    // is the only known failure mode for init()).
    catch (...) {
        if (indexWrapper) delete indexWrapper;
        catch_cpp_exception_and_throw_java(env);
    }
    return NULL;
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_gc(JNIEnv* env, jclass cls,  jlong indexPointer)
{
    try {
        IndexWrapper<float> *indexWrapper = reinterpret_cast<IndexWrapper<float>*>(indexPointer);
        has_exception_in_stack(env);
        delete indexWrapper;
        has_exception_in_stack(env);
    }
    catch (...) {
        catch_cpp_exception_and_throw_java(env);
    }
}
JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_gcI(JNIEnv* env, jclass cls,  jlong indexPointer)
{
    try {
        IndexWrapper<int> *indexWrapper = reinterpret_cast<IndexWrapper<int>*>(indexPointer);
        has_exception_in_stack(env);
        delete indexWrapper;
        has_exception_in_stack(env);
    }
    catch (...) {
        catch_cpp_exception_and_throw_java(env);
    }
}
JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex_initLibrary(JNIEnv *, jclass)
{
    initLibrary();

}
