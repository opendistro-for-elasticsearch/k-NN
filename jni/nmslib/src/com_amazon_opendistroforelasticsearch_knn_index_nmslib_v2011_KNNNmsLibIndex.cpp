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

#include <unordered_map>
#include "com_amazon_opendistroforelasticsearch_knn_index_nmslib_v2011_KNNNmsLibIndex.h"
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

// mapMetric is used to map a string from the plugin to an nmslib space. All translation should be done via this map
std::unordered_map<string, string> mapSpace = {
        {"l2", "l2"},
        {"l1", "l1"},
        {"linf", "linf"},
        {"cosinesimil", "cosinesimil"},
        {"innerproduct", "negdotprod"}
};

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


JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_nmslib_v2011_KNNNmsLibIndex_saveIndex(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath, jobjectArray algoParams, jstring spaceType)
{
    Space<float>* space = nullptr;
    ObjectVector dataset;
    Index<float>* index = nullptr;
    int* objectIds = nullptr;

    try {
        string spaceTypeCppString = knn_jni::GetStringJenv(env, spaceType);

        //TODO: Throw exception if space is not in list
        if(mapSpace.find(spaceTypeCppString) != mapSpace.end()) {
            spaceTypeCppString = mapSpace[spaceTypeCppString];
        }
        space = SpaceFactoryRegistry<float>::Instance().CreateSpace(spaceTypeCppString, AnyParams());
        objectIds = env->GetIntArrayElements(ids, nullptr);
        for (int i = 0; i < env->GetArrayLength(vectors); i++) {
            auto vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
            float* vector = env->GetFloatArrayElements(vectorArray, nullptr);
            dataset.push_back(new Object(objectIds[i], -1, env->GetArrayLength(vectorArray)*sizeof(float), vector));
            env->ReleaseFloatArrayElements(vectorArray, vector, JNI_ABORT);
        }
        // free up memory
        env->ReleaseIntArrayElements(ids, objectIds, JNI_ABORT);
        index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", spaceTypeCppString, *space, dataset);

        auto paramsList = knn_jni::GetVectorOfStrings(env, algoParams);
        string indexPathCppString = knn_jni::GetStringJenv(env, indexPath);

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
        if (objectIds) { env->ReleaseIntArrayElements(ids, objectIds, JNI_ABORT); }
        for (auto & it : dataset) {
             delete it;
        }
        delete index;
        delete space;
        knn_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_nmslib_v2011_KNNNmsLibIndex_queryIndex(JNIEnv* env, jclass cls, jlong indexPointer, jfloatArray queryVector, jint k)
{
    try {
        auto *indexWrapper = reinterpret_cast<IndexWrapper*>(indexPointer);
        float* rawQueryvector = env->GetFloatArrayElements(queryVector, nullptr);
        knn_jni::HasExceptionInStack(env);
        std::unique_ptr<const Object> queryObject(new Object(-1, -1, env->GetArrayLength(queryVector)*sizeof(float), rawQueryvector));
        env->ReleaseFloatArrayElements(queryVector, rawQueryvector, JNI_ABORT);

        KNNQuery<float> knnQuery(*(indexWrapper->space), queryObject.get(), k);
        indexWrapper->index->Search(&knnQuery);
        std::unique_ptr<KNNQueue<float>> result(knnQuery.Result()->Clone());
        int resultSize = result->Size();

        jclass resultClass = env->FindClass("com/amazon/opendistroforelasticsearch/knn/index/KNNQueryResult");
        if (resultClass == nullptr) {
            knn_jni::HasExceptionInStack(env);
        }

        jmethodID allArgs = env->GetMethodID(resultClass, "<init>", "(IF)V");
        jobjectArray results = env->NewObjectArray(resultSize, resultClass, nullptr);
        knn_jni::HasExceptionInStack(env);

        for (int i = 0; i < resultSize; i++) {
            float distance = result->TopDistance();
            long id = result->Pop()->id();
            env->SetObjectArrayElement(results, i, env->NewObject(resultClass, allArgs, id, distance));
        }
        return results;
    } catch(...) {
        knn_jni::CatchCppExceptionAndThrowJava(env);
    }
    return nullptr;
}

JNIEXPORT jlong JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_nmslib_v2011_KNNNmsLibIndex_init(JNIEnv* env, jclass cls,  jstring indexPath, jobjectArray algoParams, jstring spaceType)
{
    IndexWrapper *indexWrapper = nullptr;
    try {
        string indexPathCppString = knn_jni::GetStringJenv(env, indexPath);
        string spaceTypeCppString = knn_jni::GetStringJenv(env, spaceType);
        if(mapSpace.find(spaceTypeCppString) != mapSpace.end()) {
            spaceTypeCppString = mapSpace[spaceTypeCppString];
        }

        indexWrapper = new IndexWrapper(spaceTypeCppString);
        indexWrapper->index->LoadIndex(indexPathCppString);

        // Parse and set query params
        auto paramsList = knn_jni::GetVectorOfStrings(env, algoParams);
        indexWrapper->index->SetQueryTimeParams(AnyParams(paramsList));

        return (jlong) indexWrapper;
    }
    // nmslib seems to throw std::runtime_error if the index cannot be read (which
    // is the only known failure mode for init()).
    catch (...) {
        delete indexWrapper;
        knn_jni::CatchCppExceptionAndThrowJava(env);
    }
    return NULL;
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_nmslib_v2011_KNNNmsLibIndex_gc(JNIEnv* env, jclass cls,  jlong indexPointer)
{
    auto *indexWrapper = reinterpret_cast<IndexWrapper*>(indexPointer);
    delete indexWrapper;
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_nmslib_v2011_KNNNmsLibIndex_initLibrary(JNIEnv *, jclass)
{
    initLibrary();
}
