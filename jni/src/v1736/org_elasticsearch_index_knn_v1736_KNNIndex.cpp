#include "org_elasticsearch_index_knn_v1736_KNNIndex.h"

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

JNIEXPORT void JNICALL Java_org_elasticsearch_index_knn_v1736_KNNIndex_saveIndex(JNIEnv* env, jclass cls, jintArray ids, jobjectArray vectors, jstring indexPath)
{
    try {

        initLibrary();

        Space<float>* space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());

        ObjectVector dataset;
        int* object_ids = env->GetIntArrayElements(ids, 0);
        for (int i = 0; i < env->GetArrayLength(vectors); i++) {
            jfloatArray vectorArray = (jfloatArray)env->GetObjectArrayElement(vectors, i);
            float* vector = env->GetFloatArrayElements(vectorArray, 0);
            dataset.push_back(new Object(object_ids[i], -1, env->GetArrayLength(vectorArray)*sizeof(float), vector));
            env->ReleaseFloatArrayElements(vectorArray, vector, 0);
        }

        Index<float>* index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", "l2", *space, dataset);

        index->CreateIndex(AnyParams());
        has_exception_in_stack(env);
        index->SaveIndex(env->GetStringUTFChars(indexPath, NULL));
        has_exception_in_stack(env);
    }
    catch (...) {
        catch_cpp_exception_and_throw_java(env);
    }
}

JNIEXPORT jobjectArray JNICALL Java_org_elasticsearch_index_knn_v1736_KNNIndex_queryIndex(JNIEnv* env, jobject indexObject, jfloatArray queryVector, jint k)
{
    try {
        jclass indexClass = env->GetObjectClass(indexObject);
        jmethodID getIndex = env->GetMethodID(indexClass, "getIndex", "()J");
        jlong indexValue = env->CallLongMethod(indexObject, getIndex);
        Index<float>* index = reinterpret_cast<Index<float>*>(indexValue);
        float* vector = env->GetFloatArrayElements(queryVector, 0);
        Space<float>* space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
        KNNQuery<float> query(*space, new Object(-1, -1, env->GetArrayLength(queryVector)*sizeof(float), vector), k);
        env->ReleaseFloatArrayElements(queryVector, vector, 0);
        has_exception_in_stack(env);
        index->SetQueryTimeParams(AnyParams({ "ef=512" }));

        index->Search(&query);
        KNNQueue<float>* result = query.Result()->Clone();
        has_exception_in_stack(env);
        int resultSize = result->Size();
        jclass resultClass = env->FindClass("org/elasticsearch/index/knn/KNNQueryResult");
        jmethodID allArgs = env->GetMethodID(resultClass, "<init>", "(IF)V");
        jobjectArray results = env->NewObjectArray(resultSize, resultClass, NULL);
        for (int i = 0; i < resultSize; i++) {
            float distance = result->TopDistance();
            long id = result->Pop()->id();
            env->SetObjectArrayElement(results, i, env->NewObject(resultClass, allArgs, id, distance));
        }
        has_exception_in_stack(env);
        return results;
    }
    catch (...) {
        catch_cpp_exception_and_throw_java(env);
    }
    return NULL;
}

JNIEXPORT void JNICALL Java_org_elasticsearch_index_knn_v1736_KNNIndex_init(JNIEnv* env, jobject indexObject, jstring indexPath)
{

    try {
        initLibrary();
        Space<float>* space = SpaceFactoryRegistry<float>::Instance().CreateSpace("l2", AnyParams());
        ObjectVector* dataset = new ObjectVector();
        Index<float>* index = MethodFactoryRegistry<float>::Instance().CreateMethod(false, "hnsw", "l2", *space, *dataset);
        index->LoadIndex(env->GetStringUTFChars(indexPath, NULL));
        has_exception_in_stack(env);
        jclass indexClass = env->GetObjectClass(indexObject);
        jmethodID setIndex = env->GetMethodID(indexClass, "setIndex", "(J)V");
        env->CallVoidMethod(indexObject, setIndex, (jlong)index);
        has_exception_in_stack(env);
    }
    catch (...) {
        catch_cpp_exception_and_throw_java(env);
    }
}

JNIEXPORT void JNICALL Java_org_elasticsearch_index_knn_v1736_KNNIndex_gc(JNIEnv* env, jobject indexObject)
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