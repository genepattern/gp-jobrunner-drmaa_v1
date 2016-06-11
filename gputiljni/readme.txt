GenePattern JNI Library Implemented in C
-----------------------------------------

This library makes it possible to set system environment variables after 
the JVM is started. It was developed for the Univa Grid Engine DRMAA v1 integration 
at the Broad Institute.

See: org.genepattern.drm.util.Env for javadoc.

To build:
# create header file
javah -cp ../target/classes org.genepattern.drm.util.Env

# manually create .c file from the header
See: org_genepattern_drm_util_Env.c

# compile .dylib file on Mac OS X
gcc -I$JAVA_HOME/include -I$JAVA_HOME/include/darwin -shared -o libgputil.dylib org_genepattern_drm_util_Env.c

# compile .so file on RHEL6 node
use Java-1.8
use GCC-5.2
gcc -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -shared -o libgputil.so org_genepattern_drm_util_Env.c

# run the test program
java -Djava.library.path=. \
    -cp ../Tomcat/webapps/gp/WEB-INF/lib/gp-jobrunner-drmaa_v1-0.1-SNAPSHOT-r36.jar \
    org.genepattern.drm.util.Env MY_KEY MY_VALUE
