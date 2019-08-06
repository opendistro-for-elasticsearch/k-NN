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
        initLibrary();
        space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
        object_ids = env->GetIntArrayElements(ids, 0);
        for (int i = 0; i < env->GetArrayLength(vectors); i++) {
            jfloatArray vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
            float* vector = env->GetFloatArrayElements(vectorArray, 0);
            dataset.push_back(new Object(object_ids[i], -1, env->GetArrayLength(vectorArray)*sizeof(float), vector));
            env->ReleaseFloatArrayElements(vectorArray, vector, 0);
        }
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
        const char * indexString = env->GetStringUTFChars(indexPath, 0);
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

JNIEXPORT jobjectArray JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex_queryIndex(JNIEnv* env, jobject indexObject, jfloatArray queryVector, jint k, jobjectArray algoParams)
{
    Space<float>* space = NULL;
    KNNQueue<float>* result = NULL;
    Object* queryObject = NULL;

    try {
        jclass indexClass = env->GetObjectClass(indexObject);
        jmethodID getIndex = env->GetMethodID(indexClass, "getIndex", "()J");
        jlong indexValue = env->CallLongMethod(indexObject, getIndex);
        Index<float>* index = reinterpret_cast<Index<float>*>(indexValue);
        float* rawQueryvector = env->GetFloatArrayElements(queryVector, 0);
        space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
        queryObject = new Object(-1, -1, env->GetArrayLength(queryVector)*sizeof(float), rawQueryvector);
        KNNQuery<float> query(*space, queryObject, k);
        env->ReleaseFloatArrayElements(queryVector, rawQueryvector, 0);
        has_exception_in_stack(env);

        int paramsCount = env->GetArrayLength(algoParams);
        vector<string> paramsList;
        for (int i=0; i<paramsCount; i++) {
            jstring param = (jstring) (env->GetObjectArrayElement(algoParams, i));
            const char *rawString = env->GetStringUTFChars(param, 0);
            paramsList.push_back(rawString);
            env->ReleaseStringUTFChars(param, rawString);
        }
        index->SetQueryTimeParams(AnyParams(paramsList));
        index->Search(&query);
        result = query.Result()->Clone();
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

        //free up memory
        delete space;
        delete result;
        delete queryObject;

        return results;
    }
    catch (...) {
        if (space) { delete space; }
        if (result) { delete result; }
        if (queryObject) { delete queryObject; }
        catch_cpp_exception_and_throw_java(env);
    }
    return NULL;
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex_init(JNIEnv* env, jobject indexObject, jstring indexPath)
{
    Space<float>* space = NULL;
    ObjectVector* dataset = NULL;
    Index<float>* index = NULL;

    try {
        initLibrary();
        space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
        dataset = new ObjectVector();
        Index<float>* index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", "l2", *space, *dataset);
        const char *indexString = env->GetStringUTFChars(indexPath, 0);
        index->LoadIndex(indexString);
        env->ReleaseStringUTFChars(indexPath, indexString);
        has_exception_in_stack(env);
        jclass indexClass = env->GetObjectClass(indexObject);
        jmethodID setIndex = env->GetMethodID(indexClass, "setIndex", "(J)V");
        env->CallVoidMethod(indexObject, setIndex, (jlong)index);
        has_exception_in_stack(env);

        // free up memory
        delete space;
        delete dataset;
    }
    catch (...) {
        if (space) { delete space; }
        if (dataset) { delete dataset; }
        catch_cpp_exception_and_throw_java(env);
    }
}

JNIEXPORT void JNICALL Java_com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex_gc(JNIEnv* env, jobject indexObject)
{
    try {
        jclass indexClass = env->GetObjectClass(indexObject);
        jmethodID getIndex = env->GetMethodID(indexClass, "getIndex", "()J");
        // index heap pointer
        jlong indexValue = env->CallLongMethod(indexObject, getIndex);
        Index<float>* index = reinterpret_cast<Index<float>*>(indexValue);
        has_exception_in_stack(env);
        delete index;
        has_exception_in_stack(env);
    }
    catch (...) {
        catch_cpp_exception_and_throw_java(env);
    }
}
