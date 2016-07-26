#include "org_genepattern_drm_util_Env.h"
#include <stdlib.h>

JNIEXPORT jint JNICALL Java_org_genepattern_drm_util_Env_setenv
(JNIEnv *env, jobject object, jstring j_name, jstring j_value, jint overwrite) {
  const char *name = (*env)->GetStringUTFChars(env, j_name, 0);
  const char *value = (*env)->GetStringUTFChars(env, j_value, 0);
  int rval = setenv(name, value, overwrite);
  (*env)->ReleaseStringUTFChars(env, j_name, name);
  (*env)->ReleaseStringUTFChars(env, j_value, value);
  return rval;
}

