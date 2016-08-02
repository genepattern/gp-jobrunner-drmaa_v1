package org.genepattern.drm.impl.drmaa_v1;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.genepattern.drm.DrmJobSubmission;
import org.genepattern.server.config.GpConfig;
import org.genepattern.server.config.GpContext;
import org.genepattern.webservice.JobInfo;
import org.genepattern.webservice.TaskInfo;
import org.junit.Test;

/**
 * test custom resources in config_yaml file.
 * @author pcarr
 *
 */
public class TestCustomResourceNames {

    @Test
    public void jobGeResourceOs_custom() throws Throwable {
        final TaskInfo cleTask=new TaskInfo();
        cleTask.setName("ConvertLineEndings");

        final String configFileName="/custom_resource_names.yaml";
        final GpConfig gpConfig=TestConfigYaml.initGpConfig(configFileName);
        final GpContext jobContext=new GpContext.Builder()
            .userId("test_user")
            .taskInfo(cleTask)
            .jobInfo(new JobInfo())
        .build();
        
        final DrmaaV1JobRunner jobRunner = new DrmaaV1JobRunner();
        DrmJobSubmission jobSubmission = new DrmJobSubmission.Builder(new File("./"))
            .gpConfig(gpConfig)
            .jobContext(jobContext)
        .build();
        final List<String> actual=jobRunner.getCustomResourceFlags(jobSubmission);
        assertEquals("", 
                Arrays.asList(
                    "-l", "os=centos5", 
                    "-l", "test_wildcard=prefix.*",
                    "-l", "test_list=A|B|C"
                ), 
                actual); 
    }

}
