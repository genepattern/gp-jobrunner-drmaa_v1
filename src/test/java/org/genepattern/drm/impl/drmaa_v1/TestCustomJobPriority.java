package org.genepattern.drm.impl.drmaa_v1;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.List;

import org.genepattern.drm.DrmJobSubmission;
import org.genepattern.server.config.GpConfig;
import org.genepattern.server.config.GpContext;
import org.genepattern.webservice.JobInfo;
import org.genepattern.webservice.TaskInfo;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * test custom resources in config_yaml file.
 * @author pcarr
 *
 */
public class TestCustomJobPriority {
    private static TaskInfo cleTask;
    private static TaskInfo testStep;
    private static GpConfig configExample;

    private GpContext jobContext;
    
    @BeforeClass
    public static void beforeClass() throws Throwable {
        cleTask=new TaskInfo();
        cleTask.setName("ConvertLineEndings");
        testStep=new TaskInfo();
        testStep.setName("TestStep");
        configExample=TestConfigYaml.initGpConfig("/config_example_drmaa_v1.yaml");
    }

    @Test
    public void jobPriority_default() throws Throwable {
        jobContext=new GpContext.Builder()
            .userId("test_user")
            .taskInfo(cleTask)
            .jobInfo(new JobInfo())
        .build();
        
        final DrmaaV1JobRunner jobRunner = new DrmaaV1JobRunner();
        DrmJobSubmission job = new DrmJobSubmission.Builder(new File("./"))
            .gpConfig(configExample)
            .jobContext(jobContext)
        .build();
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertFalse("not expecting '-p' flag, args="+args, args.contains("-p"));
    }

    @Test
    public void jobPriority_perModule() throws Throwable {
        JobInfo jobInfo=new JobInfo();
        jobInfo.setTaskName("TestStep");
        jobContext=new GpContext.Builder()
            .userId("test_user")
            .taskInfo(testStep)
            .jobInfo(jobInfo)
        .build();
        
        final DrmaaV1JobRunner jobRunner = new DrmaaV1JobRunner();
        DrmJobSubmission job = new DrmJobSubmission.Builder(new File("./"))
            .gpConfig(configExample)
            .jobContext(jobContext)
        .build();
        final List<String> args=jobRunner.initNativeSpecification(job);
        TestDrmaaV1JobRunner.assertArgWithFlag(args, "-p", "-10"); 
    }

    @Test
    public void jobPriority_perUser() throws Throwable {
        jobContext=new GpContext.Builder()
            .userId("custom_user")
            .taskInfo(cleTask)
            .jobInfo(new JobInfo())
        .build();
        
        final DrmaaV1JobRunner jobRunner = new DrmaaV1JobRunner();
        DrmJobSubmission job = new DrmJobSubmission.Builder(new File("./"))
            .gpConfig(configExample)
            .jobContext(jobContext)
        .build();
        final List<String> args=jobRunner.initNativeSpecification(job);
        TestDrmaaV1JobRunner.assertArgWithFlag(args, "-p", "-30"); 
    }

}
