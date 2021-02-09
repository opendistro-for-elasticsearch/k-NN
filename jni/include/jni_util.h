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

#ifndef OPENDISTRO_KNN_JNI_UTIL_H
#define OPENDISTRO_KNN_JNI_UTIL_H

#include <jni.h>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

namespace knn_jni {
    // Takes the name of a Java exception type and a message and throws the corresponding exception
    // to the JVM
    void ThrowJavaException(JNIEnv* env, const char* type = "", const char* message = "");


    // Checks if an exception occurred in the JVM and if so throws a C++ exception
    // This should be called after some calls to JNI functions
    inline void HasExceptionInStack(JNIEnv* env)
    {
        if (env->ExceptionCheck() == JNI_TRUE) {
            throw std::runtime_error("Exception Occurred");
        }
    }


    // Catches a C++ exception and throws the corresponding exception to the JVM
    void CatchCppExceptionAndThrowJava(JNIEnv* env);


    // Returns cpp copied string from the Java string and releases the JNI Resource
    std::string GetStringJenv(JNIEnv * env, jstring javaString);


    // Returns the translation of a jobjectArray containing jstrings to a c++ vector of strings and releases the underlying
    // JNI resources
    std::vector<std::string> GetVectorOfStrings(JNIEnv * env, jobjectArray javaStringsArray);
}

#endif //OPENDISTRO_KNN_JNI_UTIL_H
