package org.genepattern.drm.impl.drmaa_v1;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.genepattern.drm.DrmJobSubmission;
import org.genepattern.server.config.GpConfig;
import org.genepattern.server.config.GpContext;
import org.genepattern.server.executor.CommandExecutorException;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SimpleJobTemplate;
import org.junit.Before;
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
            .commandLine(Arrays.asList("echo", "Hello, World!"))
        .build();

        jobRunner=new DrmaaV1JobRunner();
        session=mock(Session.class);
        when(session.createJobTemplate()).thenReturn(new SimpleJobTemplate());
    }
    
    @Test(expected=CommandExecutorException.class)
    public void validateCmdLine_nullCmdLine() throws CommandExecutorException {
        job=mock(DrmJobSubmission.class);
        when(job.getCommandLine()).thenReturn(null);
        jobRunner=new DrmaaV1JobRunner();
        jobRunner.validateCmdLine(job);
    }
    
    @Test(expected=CommandExecutorException.class)
    public void validateCmdLine_emptyCmdLine() throws CommandExecutorException {
        final List<String> cmdLine=Collections.emptyList();
        job=mock(DrmJobSubmission.class);
        when(job.getCommandLine()).thenReturn(cmdLine);
        jobRunner=new DrmaaV1JobRunner();
        jobRunner.validateCmdLine(job);
    }

    @Test
    public void validateCmdLine_oneArg() throws CommandExecutorException {
        final List<String> cmdLine=Arrays.asList("echo");
        job=mock(DrmJobSubmission.class);
        when(job.getCommandLine()).thenReturn(cmdLine);
        jobRunner=new DrmaaV1JobRunner();
        jobRunner.validateCmdLine(job);
        assertTrue("validateCmdLine should not throw an exception", true);
    }

    @Test
    public void validateCmdLine_twoArgs() throws CommandExecutorException {
        final List<String> cmdLine=Arrays.asList("echo", "Hello, World!");
        job=mock(DrmJobSubmission.class);
        when(job.getCommandLine()).thenReturn(cmdLine);
        jobRunner=new DrmaaV1JobRunner();
        jobRunner.validateCmdLine(job);
        assertTrue("validateCmdLine should not throw an exception", true);
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

}
