Local maven repository for jar files and other dependencies which are not available
in a public or private Maven repository. As described in this article:
    https://devcenter.heroku.com/articles/local-maven-dependencies

Example
-------
Here is what I did to add the drmaa.jar file from the Univa Grid Engine installation at the Broad Institute.
# 1) download the jar file
    scp {remote-user}@{remote-host}:/broad/uge/research/lib/drmaa.jar drmaa-uge-8.3.1p6.jar
# 2) add it to the local repository
    mvn deploy:deploy-file -Durl=file://`pwd` -Dfile=drmaa-uge-8.3.1p6.jar -DgroupId=com.univa -DartifactId=drmaa-uge -Dpackaging=jar -Dversion=8.3.1p6

# 3) Added these sections to the pom.xml file
  <repositories>
    <!-- local project repository for unmanaged dependencies -->
    <repository>
      <id>project.local</id>
      <name>project</name>
      <url>file:${project.basedir}/repo</url>
    </repository>
  </repositories>

  <dependencies>
  ...
     <!-- local copy of drmaa.jar from Univa Grid Engine installation -->
     <dependency>
         <groupId>com.univa</groupId>
         <artifactId>drmaa-uge</artifactId>
         <version>8.3.1p6</version>
     </dependency>
  
  </dependencies>
