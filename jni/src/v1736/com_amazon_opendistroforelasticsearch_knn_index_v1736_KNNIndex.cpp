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

#include "com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex.h"

#include "init.h"
#include "index.h"
#include "params.h"
#include "knnquery.h"
#include "knnqueue.h"
#include "methodfactory.h"
#include "spacefactory.h"
#include "space.h"

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

struct IndexWrapper {
  IndexWrapper() {
    space.reset(SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams()));
    index.reset(MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", "l2", *space, data));
  }
  std::unique_ptr<Space<float>> space;
  std::unique_ptr<Index<float>> index;
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

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex_saveIndex(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath, jobjectArray algoParams)
{
    Space<float>* space = NULL;
    ObjectVector dataset;
    Index<float>* index = NULL;
    int* object_ids = NULL;

    try {
        space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
        object_ids = env->GetIntArrayElements(ids, 0);
        for (int i = 0; i < env->GetArrayLength(vectors); i++) {
            jfloatArray vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
            float* vector = env->GetFloatArrayElements(vectorArray, 0);
            dataset.push_back(new Object(object_ids[i], -1, env->GetArrayLength(vectorArray)*sizeof(float), vector));
            env->ReleaseFloatArrayElements(vectorArray, vector, 0);
        }
        // free up memory
        env->ReleaseIntArrayElements(ids, object_ids, 0);
        index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", "l2", *space, dataset);

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

JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex_queryIndex(JNIEnv* env, jclass cls, jlong indexPointer, jfloatArray queryVector, jint k)
{
    IndexWrapper *indexWrapper = reinterpret_cast<IndexWrapper*>(indexPointer);

    float* rawQueryvector = env->GetFloatArrayElements(queryVector, 0);
    std::unique_ptr<const Object> queryObject(new Object(-1, -1, env->GetArrayLength(queryVector)*sizeof(float), rawQueryvector));
    env->ReleaseFloatArrayElements(queryVector, rawQueryvector, 0);

    KNNQuery<float> knnQuery(*(indexWrapper->space), queryObject.get(), k);
    indexWrapper->index->Search(&knnQuery);
    std::unique_ptr<KNNQueue<float>> result(knnQuery.Result()->Clone());
    int resultSize = result->Size();
    jclass resultClass = env->FindClass("com/amazon/opendistroforelasticsearch/knn/index/KNNQueryResult");
    jmethodID allArgs = env->GetMethodID(resultClass, "<init>", "(IF)V");
    jobjectArray results = env->NewObjectArray(resultSize, resultClass, NULL);
    for (int i = 0; i < resultSize; i++) {
      float distance = result->TopDistance();
      long id = result->Pop()->id();
      env->SetObjectArrayElement(results, i, env->NewObject(resultClass, allArgs, id, distance));
    }
    return results;
}

JNIEXPORT jlong JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex_init(JNIEnv* env, jclass cls,  jstring indexPath, jobjectArray algoParams)
{
    IndexWrapper *indexWrapper = NULL;
    try {
        const char *indexPathCStr = env->GetStringUTFChars(indexPath, 0);
        string indexPathString(indexPathCStr);
        env->ReleaseStringUTFChars(indexPath, indexPathCStr);

        // Load index from file (may throw)
        IndexWrapper *indexWrapper = new IndexWrapper();
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

        return (jlong) indexWrapper;
    }
    // nmslib seems to throw std::runtime_error if the index cannot be read (which
    // is the only known failure mode for init()).
    catch (const std::runtime_error& re) {
        if (indexWrapper) delete indexWrapper;
        JavaException(env, "java/lang/IOException", re.what());
    }
    return NULL;
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex_gc(JNIEnv* env, jclass cls,  jlong indexPointer)
{
    IndexWrapper *indexWrapper = reinterpret_cast<IndexWrapper*>(indexPointer);
    delete indexWrapper;
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex_initLibrary(JNIEnv *, jclass)
{
    initLibrary();

}
