package org.genepattern.drm.impl.drmaa_v1;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.math.BigDecimal;
import java.net.URL;

import org.genepattern.server.config.GpConfig;
import org.genepattern.server.config.GpContext;
import org.genepattern.webservice.TaskInfo;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test site customization in the config_yaml file.
 * @author pcarr
 *
 */
public class TestConfigYaml {
    private static GpConfig configExample;
    private static GpContext jobContext_default;
    private static TaskInfo cleTask;
    private static TaskInfo testStep;
    
    /** utility method: create new GpConfig instance from given config_yaml file */
    protected static final GpConfig initGpConfig(final String filename) throws Throwable {
        final URL url=TestConfigYaml.class.getResource(filename);
        if (url==null) {
            throw new Exception("failed to getResource("+filename+")");
        }
        final File configFile=new File(url.getFile());
        final GpConfig gpConfig=new GpConfig.Builder()
            .configFile(configFile)
        .build();
        if (gpConfig.hasInitErrors()) {
            throw gpConfig.getInitializationErrors().get(0);
        }
        return gpConfig;
    }

    @BeforeClass
    public static void beforeClass() throws Throwable {
        cleTask=new TaskInfo();
        cleTask.setName("ConvertLineEndings");
        testStep=new TaskInfo();
        testStep.setName("TestStep");
        configExample=initGpConfig("/config_example_drmaa_v1.yaml");
    }
    
    @Before
    public void setUp() throws Throwable {
        jobContext_default=new GpContext.Builder()
            .userId("test_user")
            .taskInfo(cleTask)
        .build();
    }
    
    /**
     * test-case: set job.ge.clear to true in executor.default.properties
     * <pre>
        default.properties:
            job.ge.clear: true
     * </pre>
     * @throws Throwable
     */
    @Test
    public void setDefaultProp_jobGeClear_toTrue() throws Throwable {
        final String configFileName="/config_test_01.yml";
        final GpConfig gpConfig=initGpConfig(configFileName);
        assertEquals(configFileName+": getGPBooleanProperty('"+DrmaaV1JobRunner.PROP_CLEAR+"')", 
                true, 
                gpConfig.getGPBooleanProperty(jobContext_default, DrmaaV1JobRunner.PROP_CLEAR));
    }

    /**
     * test-case: set job.ge.clear to false in executor.default.properties
     * <pre>
        default.properties:
            job.ge.clear: false
     * </pre>
     * @throws Throwable
     */
    @Test
    public void setDefaultProp_jobGeClear_toFalse() throws Throwable {
        final String configFileName="/config_test_02.yml";
        final GpConfig gpConfig=initGpConfig(configFileName);
        assertEquals(configFileName+": getGPBooleanProperty('"+DrmaaV1JobRunner.PROP_CLEAR+"')", 
                false, 
                gpConfig.getGPBooleanProperty(jobContext_default, DrmaaV1JobRunner.PROP_CLEAR));
    }
    
    @Test
    public void jobPriority_default() throws Throwable { 
        assertEquals("job.priority", 
                null, 
                DrmaaV1JobRunner.getGPBigDecimalProperty(configExample, jobContext_default, "job.priority"));
    }

    @Test
    public void jobGeClear_default() throws Throwable { 
        assertEquals(DrmaaV1JobRunner.PROP_CLEAR, 
                false, 
                configExample.getGPBooleanProperty(jobContext_default, DrmaaV1JobRunner.PROP_CLEAR));
    }
    
    @Test
    public void jobPriority_perModule() throws Throwable { 
        final GpContext jobContext_custom=new GpContext.Builder()
            .userId("test_user")
            .taskInfo(testStep)
        .build();
        assertEquals("job.priority", 
                new BigDecimal("-10"), 
                DrmaaV1JobRunner.getGPBigDecimalProperty(configExample, jobContext_custom, "job.priority"));
    }

    @Test 
    public void jobGeClear_perModule() throws Throwable {
        final GpContext jobContext_custom=new GpContext.Builder()
            .userId("test_user")
            .taskInfo(testStep)
        .build();
        assertEquals("getGPBooleanProperty("+DrmaaV1JobRunner.PROP_CLEAR+")", 
                true, 
                configExample.getGPBooleanProperty(jobContext_custom, DrmaaV1JobRunner.PROP_CLEAR));
    }
    
    @Test 
    public void jobPriority_perUser() throws Throwable {
        final GpContext jobContext_custom=new GpContext.Builder()
            .userId("custom_user")
        .build();
        assertEquals("job.priority", 
                new BigDecimal("-30"), 
                DrmaaV1JobRunner.getGPBigDecimalProperty(configExample, jobContext_custom, "job.priority"));
    }

    @Test 
    public void jobGeClear_perUser() throws Throwable {
        final GpContext jobContext_custom=new GpContext.Builder()
            .userId("custom_user")
        .build();
        assertEquals("getGPBooleanProperty("+DrmaaV1JobRunner.PROP_CLEAR+")", 
                true, 
                configExample.getGPBooleanProperty(jobContext_custom, DrmaaV1JobRunner.PROP_CLEAR));
    }

}
