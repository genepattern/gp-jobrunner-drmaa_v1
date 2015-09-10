package org.genepattern.drm.impl.drmaa_v1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.genepattern.drm.DrmJobSubmission;

public class Util {
    private static final Logger log = Logger.getLogger(Util.class);
    
    protected static boolean getGPBooleanProperty(final DrmJobSubmission jobSubmission, final String key, final boolean defaultValue) {
        return jobSubmission.getGpConfig().getGPBooleanProperty(jobSubmission.getJobContext(), key, defaultValue);
    }
    
    /**
     * If configured by the server admin, write the command line into a log file in the working directory for the job.
     * Example config,
     * <pre>
           # when this property is set, output the command line into the log file
           job.logFile: .lsf.out
     * </pre>
     * 
     * @param jobSubmission
     * @return true the file that was written to, or null if not logged
     */
    public static File logCommandLine(final DrmJobSubmission jobSubmission) {
        if (log.isDebugEnabled()) {
            log.debug("commandLine="+jobSubmission.getCommandLine());
        }
        final File logFile=jobSubmission.getRelativeFile(jobSubmission.getLogFile());
        if (logFile == null) {
            log.debug("logFile==null");
            return null;
        }
        if (logFile.exists()) {
            log.error("log file already exists: "+logFile.getAbsolutePath());
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("logging cmdLine to file ... "+logFile.getAbsolutePath());
        }
        
        BufferedWriter bw = null;
        try {
            FileWriter fw = new FileWriter(logFile);
            bw = new BufferedWriter(fw);
            bw.write(jobSubmission.getCommandLine().toString());
            bw.newLine();
            int i=0;
            for(final String arg : jobSubmission.getCommandLine()) {
                bw.write("    arg["+i+"]: '"+arg+"'");
                bw.newLine();
                ++i;
            }
            bw.close();
            return logFile;
        }
        catch (Throwable t) {
            log.error("error logging cmdLine to file: "+logFile.getAbsolutePath(), t);
            return null;
        }
        finally {
            if (bw != null) {
                try {
                    bw.close();
                }
                catch (IOException e) {
                    log.error(e);
                }
            }
        }
    }

}
