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

#include "com_amazon_opendistroforelasticsearch_knn_index_v2011_KNNIndex.h"
#include "jni_util.h"

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

struct IndexWrapper {
  explicit IndexWrapper(const string& spaceType) {
    // Index gets constructed with a reference to data (see above) but is otherwise unused
    ObjectVector data;
    space.reset(SpaceFactoryRegistry<float>::Instance().CreateSpace(spaceType, AnyParams()));
    index.reset(MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", spaceType, *space, data));
  }
  std::unique_ptr<Space<float>> space;
  std::unique_ptr<Index<float>> index;
};

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v2011_KNNIndex_saveIndex(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath, jobjectArray algoParams, jstring spaceType)
{
    Space<float>* space = nullptr;
    ObjectVector dataset;
    Index<float>* index = nullptr;
    int* objectIds = nullptr;

    try {
        string spaceTypeCppString = getStringJenv(env, spaceType);
        space = SpaceFactoryRegistry<float>::Instance().CreateSpace(spaceTypeCppString, AnyParams());
        objectIds = env->GetIntArrayElements(ids, nullptr);
        for (int i = 0; i < env->GetArrayLength(vectors); i++) {
            auto vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
            float* vector = env->GetFloatArrayElements(vectorArray, nullptr);
            dataset.push_back(new Object(objectIds[i], -1, env->GetArrayLength(vectorArray)*sizeof(float), vector));
            env->ReleaseFloatArrayElements(vectorArray, vector, 0);
        }
        // free up memory
        env->ReleaseIntArrayElements(ids, objectIds, 0);
        index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", spaceTypeCppString, *space, dataset);

        int paramsCount = env->GetArrayLength(algoParams);
        vector<string> paramsList;
        string paramString;
        for (int i=0; i<paramsCount; i++) {
            paramString = getStringJenv(env, (jstring)(env->GetObjectArrayElement(algoParams, i)));
            paramsList.push_back(paramString);
        }

        string indexPathCppString = getStringJenv(env, indexPath);
        index->CreateIndex(AnyParams(paramsList));
        index->SaveIndex(indexPathCppString);

        // Free each object in the dataset. No need to clear the vector because it goes out of scope
        // immediately
        for (auto & it : dataset) {
             delete it;
        }
        delete index;
        delete space;
    }
    catch (...) {
        if (objectIds) { env->ReleaseIntArrayElements(ids, objectIds, 0); }
        for (auto & it : dataset) {
             delete it;
        }
        if (index) { delete index; }
        if (space) { delete space; }
        catch_cpp_exception_and_throw_java(env);
    }
}

JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v2011_KNNIndex_queryIndex(JNIEnv* env, jclass cls, jlong indexPointer, jfloatArray queryVector, jint k)
{
    try {
        auto *indexWrapper = reinterpret_cast<IndexWrapper*>(indexPointer);

        float* rawQueryvector = env->GetFloatArrayElements(queryVector, nullptr);
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
        jobjectArray results = env->NewObjectArray(resultSize, resultClass, nullptr);
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
    return nullptr;
}

JNIEXPORT jlong JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v2011_KNNIndex_init(JNIEnv* env, jclass cls,  jstring indexPath, jobjectArray algoParams, jstring spaceType)
{
    IndexWrapper *indexWrapper = nullptr;
    try {
        string indexPathCppString = getStringJenv(env, indexPath);
        string spaceTypeCppString = getStringJenv(env, spaceType);

        indexWrapper = new IndexWrapper(spaceTypeCppString);
        indexWrapper->index->LoadIndex(indexPathCppString);

        // Parse and set query params
        int paramsCount = env->GetArrayLength(algoParams);
        vector<string> paramsList;
        string paramString;
        for (int i=0; i<paramsCount; i++) {
            paramString = getStringJenv(env, (jstring)(env->GetObjectArrayElement(algoParams, i)));
            paramsList.push_back(paramString);
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

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v2011_KNNIndex_gc(JNIEnv* env, jclass cls,  jlong indexPointer)
{
    try {
        auto *indexWrapper = reinterpret_cast<IndexWrapper*>(indexPointer);
        has_exception_in_stack(env);
        delete indexWrapper;
        has_exception_in_stack(env);
    }
    catch (...) {
        catch_cpp_exception_and_throw_java(env);
    }
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v2011_KNNIndex_initLibrary(JNIEnv *, jclass)
{
    initLibrary();

}
