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
import org.genepattern.drm.Memory;
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
        jobRunner.validateCmdLine(job);
    }
    
    @Test(expected=CommandExecutorException.class)
    public void validateCmdLine_emptyCmdLine() throws CommandExecutorException {
        final List<String> cmdLine=Collections.emptyList();
        job=mock(DrmJobSubmission.class);
        when(job.getCommandLine()).thenReturn(cmdLine);
        jobRunner.validateCmdLine(job);
    }

    @Test
    public void validateCmdLine_oneArg() throws CommandExecutorException {
        final List<String> cmdLine=Arrays.asList("echo");
        job=mock(DrmJobSubmission.class);
        when(job.getCommandLine()).thenReturn(cmdLine);
        jobRunner.validateCmdLine(job);
        assertTrue("validateCmdLine should not throw an exception", true);
    }

    @Test
    public void validateCmdLine_twoArgs() throws CommandExecutorException {
        final List<String> cmdLine=Arrays.asList("echo", "Hello, World!");
        job=mock(DrmJobSubmission.class);
        when(job.getCommandLine()).thenReturn(cmdLine);
        jobRunner.validateCmdLine(job);
        assertTrue("validateCmdLine should not throw an exception", true);
    }
    
    protected void assertArgWithFlag(final List<String> args, final String flag, final String expected) {
        int idx0=args.indexOf(flag);
        assertTrue("checking for '"+flag+"' in args="+args, idx0>=0);
        assertEquals("checking for '"+expected+"' in args="+args, expected, args.get(idx0+1));
    }
    
    @Test
    public void stdout_null() {
        job=mock(DrmJobSubmission.class);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-o", "stdout.txt");
    }
    
    @Test
    public void stderr_null() {
        job=mock(DrmJobSubmission.class);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-e", "stderr.txt");
    }

    @Test
    public void stdin_null() {
        job=mock(DrmJobSubmission.class);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertFalse("not expecting '-i' flag", args.contains("-i"));
    }

    @Test
    public void stdout_relativePath() {
        job=mock(DrmJobSubmission.class);
        when(job.getStdoutFile()).thenReturn(new File(".custom_stdout.txt"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-o", ".custom_stdout.txt");
    }
    
    @Test
    public void stderr_relativePath() {
        job=mock(DrmJobSubmission.class);
        when(job.getStderrFile()).thenReturn(new File(".custom_stderr.txt"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-e", ".custom_stderr.txt");
    }

    @Test
    public void stdin_relativePath() {
        job=mock(DrmJobSubmission.class);
        when(job.getStdinFile()).thenReturn(new File(".custom_stdin.txt"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-i", ".custom_stdin.txt");
    }

    @Test
    public void stdout_fullPathToJobDir() {
        job=mock(DrmJobSubmission.class);
        when(job.getWorkingDir()).thenReturn(jobDir);
        when(job.getStdoutFile()).thenReturn(new File(jobDir, "stdout.txt"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-o", "stdout.txt");
    }
    
    @Test
    public void stderr_fullPathToJobDir() {
        job=mock(DrmJobSubmission.class);
        when(job.getWorkingDir()).thenReturn(jobDir);
        when(job.getStderrFile()).thenReturn(new File(jobDir, "stderr.txt"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-e", "stderr.txt");
    }

    @Test
    public void stdin_fullPathToJobDir() {
        job=mock(DrmJobSubmission.class);
        when(job.getWorkingDir()).thenReturn(jobDir);
        when(job.getStdinFile()).thenReturn(new File(jobDir, "stdin.txt"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-i", "stdin.txt");
    }

    @Test
    public void stdout_fqPath() throws IOException {
        File stdoutFile=temp.newFile(".custom_stdout.txt");
        job=mock(DrmJobSubmission.class);
        when(job.getWorkingDir()).thenReturn(jobDir);
        when(job.getStdoutFile()).thenReturn(stdoutFile);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-o", stdoutFile.getPath()); 
    }
    
    @Test
    public void stderr_fqPath() throws IOException {
        File stderrFile=temp.newFile(".custom_stderr.txt");
        job=mock(DrmJobSubmission.class);
        when(job.getWorkingDir()).thenReturn(jobDir);
        when(job.getStderrFile()).thenReturn(stderrFile);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-e", stderrFile.getPath()); 
    }

    @Test
    public void stdin_fqPath() throws IOException {
        File stdinFile=temp.newFile(".custom_stdin.txt");
        job=mock(DrmJobSubmission.class);
        when(job.getWorkingDir()).thenReturn(jobDir);
        when(job.getStdinFile()).thenReturn(stdinFile);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertArgWithFlag(args, "-i", stdinFile.getPath()); 
    }
    
    @Test
    public void jobMemory() {
        job=mock(DrmJobSubmission.class);
        when(job.getMemory()).thenReturn(Memory.fromString("24 Gb"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        
        //  -l m_mem_free=Xg
        assertArgWithFlag(args, "-l", "m_mem_free=24g");
    }
    
    @Test
    public void jobMemory_lessThanOne() {
        job=mock(DrmJobSubmission.class);
        when(job.getMemory()).thenReturn(Memory.fromString("512m"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        
        //  -l m_mem_free=Xg
        assertArgWithFlag(args, "-l", "m_mem_free=1g");
    }

    @Test
    public void jobMemory_fractionalGb() {
        job=mock(DrmJobSubmission.class);
        when(job.getMemory()).thenReturn(Memory.fromString("4.5 Gb"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        
        //  -l m_mem_free=Xg
        assertArgWithFlag(args, "-l", "m_mem_free=5g");
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
