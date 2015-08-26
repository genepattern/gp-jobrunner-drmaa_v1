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
import org.genepattern.drm.JobRunner;
import org.genepattern.drm.Memory;
import org.genepattern.server.config.GpConfig;
import org.genepattern.server.config.GpContext;
import org.genepattern.server.executor.CommandExecutorException;
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
    
    protected void assertArgWithFlag(final List<String> args, final String flag, final String... expectedArgs) {
        int idx0=args.indexOf(flag);
        assertTrue("missing expected flag='"+flag+"' in args="+args, idx0>=0);
        int j=idx0;
        for(final String expected : expectedArgs) {
            ++j;
            assertEquals("checking for '"+expected+"' in args="+args, expected, args.get(j));
        }
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
        assertFalse("not expecting '-i' flag, args="+args, args.contains("-i"));
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
    public void jobQueue() {
        job=mock(DrmJobSubmission.class);
        when(job.getQueue()).thenReturn("short");
        final List<String> args=jobRunner.initNativeSpecification(job);
        // -q <queue>
        assertArgWithFlag(args, "-q", "short"); 
    }

    @Test
    public void jobQueue_emptyString() {
        job=mock(DrmJobSubmission.class);
        when(job.getQueue()).thenReturn("");
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertFalse("not expecting '-q' flag, args="+args, args.contains("-q"));
    }

    @Test
    public void jobQueue_null() {
        job=mock(DrmJobSubmission.class);
        when(job.getQueue()).thenReturn(null);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertFalse("not expecting '-q' flag, arg="+args, args.contains("-q"));
    }
    
    @Test
    public void jobProject() {
        job=mock(DrmJobSubmission.class);
        when(job.getProperty(JobRunner.PROP_PROJECT)).thenReturn("my_project");
        final List<String> args=jobRunner.initNativeSpecification(job);
        // -P <job.project>
        assertArgWithFlag(args, "-P", "my_project"); 
    }

    @Test
    public void jobProject_emptyString() {
        job=mock(DrmJobSubmission.class);
        when(job.getProperty(JobRunner.PROP_PROJECT)).thenReturn("");
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertFalse("not expecting '-P' flag, args="+args, args.contains("-P"));
    }

    @Test
    public void jobProject_null() {
        job=mock(DrmJobSubmission.class);
        when(job.getProperty(JobRunner.PROP_PROJECT)).thenReturn(null);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertFalse("not expecting '-P' flag, args="+args, args.contains("-P"));
    }

    @Test
    public void extraArgs_bindingExample() {
        job=mock(DrmJobSubmission.class);
        //when(job.getCpuCount()).thenReturn(8);
        //when(job.getExtraArgs()).thenReturn(Arrays.asList("-binding", "pe", "linear:<job.cpuCount>"));
        when(job.getExtraArgs()).thenReturn(Arrays.asList("-binding", "pe", "linear:8"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        // -pe smp 8
        assertArgWithFlag(args, "-binding", "pe", "linear:8");
    }
    
    // feature request: enable command substitutions on right-hand values in config file
    @Ignore @Test
    public void extraArgs_bindingExample_withSubstitution() {
        job=mock(DrmJobSubmission.class);
        when(job.getCpuCount()).thenReturn(8);
        when(job.getExtraArgs()).thenReturn(Arrays.asList("-binding", "pe", "linear:<job.cpuCount>"));
        final List<String> args=jobRunner.initNativeSpecification(job);
        // -pe smp 8
        assertArgWithFlag(args, "-binding", "pe", "linear:8");
    }

    /** Parallel Environment flag */
    @Test
    public void jobPeFlag_fromCpuCount() {
        // qsub -pe openmpi 8
        job=mock(DrmJobSubmission.class);
        when(job.getCpuCount()).thenReturn(8);
        final List<String> args=jobRunner.initNativeSpecification(job);
        // -pe smp 8
        assertArgWithFlag(args, "-pe", "smp", "8");
    }

    @Test
    public void jobPeFlag_fromNodeCount() {
        // qsub -pe openmpi 8
        job=mock(DrmJobSubmission.class);
        when(job.getNodeCount()).thenReturn(8);
        final List<String> args=jobRunner.initNativeSpecification(job);
        // -pe smp 8
        assertArgWithFlag(args, "-pe", "smp", "8");
    }

    @Test
    public void jobPeFlag_peType_custom() {
        // mpi, openmpi, smp
        job=mock(DrmJobSubmission.class);
        when(job.getProperty(DrmaaV1JobRunner.PROP_PE_TYPE)).thenReturn("mpi");
        when(job.getCpuCount()).thenReturn(8);
        final List<String> args=jobRunner.initNativeSpecification(job);
        // -pe smp 8
        assertArgWithFlag(args, "-pe", "mpi", "8");
    }

    @Test
    public void jobPeFlag_fromCpuCount_invalid() {
        // qsub -pe openmpi 8
        job=mock(DrmJobSubmission.class);
        when(job.getCpuCount()).thenReturn(-1);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertFalse("not expecting '-pe' flag, arg="+args, args.contains("-pe"));
    }

    @Test
    public void jobPeFlag_notSet() {
        job=mock(DrmJobSubmission.class);
        final List<String> args=jobRunner.initNativeSpecification(job);
        assertFalse("not expecting '-pe' flag, arg="+args, args.contains("-pe"));
    }

    @Test
    public void numCores_default() {
        final GpConfig gpConfig=new GpConfig.Builder().build(); 
        final DrmJobSubmission job=new DrmJobSubmission.Builder(jobDir)
            .jobContext(jobContext)
            .gpConfig(gpConfig)
        .build();

        assertEquals("numCores, default", null, jobRunner.getNumCores(job));
    }
    
    @Test
    public void numCores_fromCpuCount() {
        final GpConfig gpConfig=new GpConfig.Builder()
            .addProperty(JobRunner.PROP_CPU_COUNT, "8")
        .build();
        
        final DrmJobSubmission job=new DrmJobSubmission.Builder(jobDir)
            .jobContext(jobContext)
            .gpConfig(gpConfig)
        .build();

        assertEquals("numCores, default", new Integer(8), jobRunner.getNumCores(job));
    }

    @Test
    public void numCores_fromNodeCount() {
        final GpConfig gpConfig=new GpConfig.Builder()
            .addProperty(JobRunner.PROP_NODE_COUNT, "4")
        .build();
        
        final DrmJobSubmission job=new DrmJobSubmission.Builder(jobDir)
            .jobContext(jobContext)
            .gpConfig(gpConfig)
        .build();

        assertEquals("numCores, default", new Integer(4), jobRunner.getNumCores(job));
    }
    
    @Test 
    public void numCores_both_same() {
        final GpConfig gpConfig=new GpConfig.Builder()
            .addProperty(JobRunner.PROP_CPU_COUNT, "8")
            .addProperty(JobRunner.PROP_NODE_COUNT, "8")
        .build();
        
        final DrmJobSubmission job=new DrmJobSubmission.Builder(jobDir)
            .jobContext(jobContext)
            .gpConfig(gpConfig)
        .build();

        assertEquals("numCores, default", new Integer(8), jobRunner.getNumCores(job));
    }

    @Test 
    public void numCores_both_diff_useNodeCount() {
        final GpConfig gpConfig=new GpConfig.Builder()
            .addProperty(JobRunner.PROP_CPU_COUNT, "4")
            .addProperty(JobRunner.PROP_NODE_COUNT, "8")
        .build();
        
        final DrmJobSubmission job=new DrmJobSubmission.Builder(jobDir)
            .jobContext(jobContext)
            .gpConfig(gpConfig)
        .build();

        assertEquals("numCores, default", new Integer(8), jobRunner.getNumCores(job));
    }
    
    @Test 
    public void numCores_both_diff_useCpuCount() {
        final GpConfig gpConfig=new GpConfig.Builder()
            .addProperty(JobRunner.PROP_CPU_COUNT, "8")
            .addProperty(JobRunner.PROP_NODE_COUNT, "4")
        .build();
        
        final DrmJobSubmission job=new DrmJobSubmission.Builder(jobDir)
            .jobContext(jobContext)
            .gpConfig(gpConfig)
        .build();

        assertEquals("numCores, default", new Integer(8), jobRunner.getNumCores(job));
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
