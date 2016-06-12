package org.genepattern.drm.impl.drmaa_v1;


import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import org.genepattern.drm.DrmJobSubmission;
import org.genepattern.server.config.GpConfig;
import org.genepattern.server.config.GpContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestUtil {
    private GpConfig gpConfig;
    private GpContext jobContext;
    private DrmJobSubmission job;
    private DrmJobSubmission.Builder jobBuilder;
    private File jobDir;
    private int jobNo=1;

    @Rule
    public TemporaryFolder temp= new TemporaryFolder();
    
    @Before
    public void setUp() throws IOException {
        gpConfig=new GpConfig.Builder().build();
        jobDir=temp.newFolder(""+jobNo);
        final org.genepattern.webservice.JobInfo jobInfo=mock(org.genepattern.webservice.JobInfo.class);
        when(jobInfo.getJobNumber()).thenReturn(jobNo);
        jobContext=new GpContext.Builder()
            .jobNumber(jobNo)
            .jobInfo(jobInfo)
        .build();
        jobBuilder=new DrmJobSubmission.Builder(jobDir)
            .gpConfig(gpConfig)
            .jobContext(jobContext)
            .commandLine(Arrays.asList("echo", "Hello, World!"));
    }

    @Test
    public void skipWhenLogfileNotSet() {
        job=jobBuilder.build();
        assertEquals("job.workingDir", jobDir, job.getWorkingDir());
        final File actual=Util.logCommandLine(job);
        assertEquals("logCommandLine when 'job.logFile' is not set", null, actual);
    }
    
    @Test
    public void relativeLogFile() throws FileNotFoundException {
        File expected=new File(jobDir, ".log.out");
        jobBuilder.logFilename(".log.out");
        job=jobBuilder.build();
        final File actual=Util.logCommandLine(job);
        assertEquals("logCommandLine when job.logFilename is '.log.out'", expected, actual);
        
        final String NL="\n";
        final String expectedContent="[echo, Hello, World!]"+NL+
                "    arg[0]: 'echo'"+NL+
                "    arg[1]: 'Hello, World!'";
        final String actualContent = new Scanner(expected).useDelimiter("\\Z").next();
        assertEquals("", expectedContent, actualContent);
    }
    
    @Test
    public void prependPath_null() {
        final String element="/lib/mylib";
        final String path=null;
        
        assertEquals("path is null", element, Util.prependPath(element, path));
    }

    public void prependPath_empty() {
        final String element="/lib/mylib";
        final String path="";
        
        assertEquals("path is empty", element, Util.prependPath(element, path));
    }

    @Test
    public void prependPath() {
        final String element="/lib/mylib";
        final String path="/opt/java/lib";
        assertEquals("prependPath('"+element+"', '"+path+"')", 
                "/lib/mylib"+File.pathSeparator+"/opt/java/lib", 
                Util.prependPath(element, path));
    }

    @Test
    public void prependPath_ignoreDuplicate() {
        final String element="/lib/mylib";
        final String path="/lib/mylib:/opt/java/lib";
        assertEquals("prependPath('"+element+"', '"+path+"')", path, Util.prependPath(element, path));
    }
    
    @Test
    public void prependPath_handleSimilar() {
        final String element="/lib/mylib";
        final String path="/lib/mylib_01:/opt/java/lib";
        assertEquals("prependPath('"+element+"', '"+path+"')", 
                element+File.pathSeparator+path, Util.prependPath(element, path));
    }

}
