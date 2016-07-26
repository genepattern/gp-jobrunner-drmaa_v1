# gp-jobrunner-drmaa_v1
A GenePattern JobRunner for GridEngine integration.


This has a local maven dependency on the GenePattern Server jar file
    ./Tomcat/webapps/gp/WEB-INF/lib/gp-full.jar

I manually deployed the 'gp-full.jar' file into my local Maven repository,
    /Broad/dev/maven-repo

Example mvn command line:
    mvn deploy:deploy-file \
        -Durl=file:///Broad/dev/maven-repo/ \
        -Dfile=gp-full-3.9.8-140.jar \
        -DgroupId=org.genepattern \
        -DartifactId=gp-full \
        -Dpackaging=jar \
        -Dversion=3.9.8

