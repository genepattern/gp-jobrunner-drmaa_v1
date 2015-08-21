package org.genepattern.drm.impl.drmaa_v1;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.genepattern.drm.DrmJobSubmission;
import org.genepattern.server.config.GpConfig;
import org.genepattern.server.config.GpContext;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SimpleJobTemplate;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDrmaaV1JobRunner {
    private GpConfig gpConfig;
    private GpContext jobContext;
    private DrmJobSubmission job;
    
    private DrmaaV1JobRunner jobRunner;    
    private File jobDir;
    private int jobNo=10357;
    
    private Session session;
    
    @Rule
    public TemporaryFolder temp= new TemporaryFolder();
    
    @Before
    public void setUp() throws IOException, DrmaaException {
        gpConfig=new GpConfig.Builder().build();
        jobDir=temp.newFolder(""+jobNo);
        final org.genepattern.webservice.JobInfo jobInfo=mock(org.genepattern.webservice.JobInfo.class);
        when(jobInfo.getJobNumber()).thenReturn(jobNo);
        jobContext=new GpContext.Builder()
            .jobNumber(jobNo)
            .jobInfo(jobInfo)
        .build();
        job=new DrmJobSubmission.Builder(jobDir)
            .gpConfig(gpConfig)
            .jobContext(jobContext)
        .build();

        jobRunner=new DrmaaV1JobRunner();
        session=mock(Session.class);
        when(session.createJobTemplate()).thenReturn(new SimpleJobTemplate());
    }
    
    @Test
    public void initJobTemplate_setWorkingDir() throws DrmaaException {
        JobTemplate jt=jobRunner.initJobTemplate(session, job);
        assertEquals("jt.workingDirectory", jobDir.getAbsolutePath(), jt.getWorkingDirectory());
    }
    
    @Test
    public void initJobTemplate_getJobName() throws DrmaaException {
        JobTemplate jt=jobRunner.initJobTemplate(session, job);
        assertEquals("jt.jobName", "GP_"+jobNo, jt.getJobName()); 
    }
    
    public void initJobTemplate_ioRedirect() throws DrmaaException {
        JobTemplate jt=jobRunner.initJobTemplate(session, job);
    }

}
