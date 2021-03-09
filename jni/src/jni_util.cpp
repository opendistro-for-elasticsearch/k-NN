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

#include "jni_util.h"

#include <jni.h>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>


void knn_jni::ThrowJavaException(JNIEnv* env, const char* type, const char* message) {
    jclass newExcCls = env->FindClass(type);
    if (newExcCls != nullptr) {
        env->ThrowNew(newExcCls, message);
    }
    // If newExcCls isn't found, NoClassDefFoundError will be thrown
}


void knn_jni::CatchCppExceptionAndThrowJava(JNIEnv* env)
{
    try {
        throw;
    }
    catch (const std::bad_alloc& rhs) {
        ThrowJavaException(env, "java/io/IOException", rhs.what());
    }
    catch (const std::runtime_error& re) {
        ThrowJavaException(env, "java/lang/Exception", re.what());
    }
    catch (const std::exception& e) {
        ThrowJavaException(env, "java/lang/Exception", e.what());
    }
    catch (...) {
        ThrowJavaException(env, "java/lang/Exception", "Unknown exception occurred");
    }
}


std::string knn_jni::GetStringJenv(JNIEnv * env, jstring javaString) {
    //TODO: Add check here for javaString is null
    const char *cString = env->GetStringUTFChars(javaString, nullptr);
    if (cString == nullptr) {
        HasExceptionInStack(env);
    }
    std::string cppString(cString);
    env->ReleaseStringUTFChars(javaString, cString);
    return cppString;
}


std::vector<std::string> knn_jni::GetVectorOfStrings(JNIEnv * env, jobjectArray javaStringsArray) {
    int arraySize = env->GetArrayLength(javaStringsArray);
    std::vector<std::string> stringVector;
    std::string cppString;

    for (int i=0; i < arraySize; i++) {
        cppString = GetStringJenv(env, (jstring)(env->GetObjectArrayElement(javaStringsArray, i)));
        stringVector.push_back(cppString);
    }

    return stringVector;
}
