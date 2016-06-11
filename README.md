# gp-jobrunner-drmaa_v1
A GenePattern JobRunner for GridEngine integration.

To build the jar file:
    mvn package

To deploy to a GP server:
    scp target/gp-jobrunner-drmaa_v1-0.1-SNAPSHOT-<version>.jar {gp-role-account}@{gp-host}/{gp-install-dir}/Tomcat/webapps/gp/WEB-INF/lib

