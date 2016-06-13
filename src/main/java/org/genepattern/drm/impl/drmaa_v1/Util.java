package org.genepattern.drm.impl.drmaa_v1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.genepattern.drm.DrmJobSubmission;

import com.google.common.base.Strings;

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

    /**
     * Prepend an element to the beginning of a file path. 
     * For example to add another location to the LD_LIBRARY_PATH,
     *     String newPath = prependPath("/new/location", System.getProperty("java.library.path"));
     * 
     * @param pathElement - the element to add to the initial path
     * @param pathIn
     * @return
     */
    protected static String prependPath(final String pathElement, final String pathIn) {
        if (Strings.isNullOrEmpty(pathIn)) {
            return pathElement;
        }
        if (Strings.isNullOrEmpty(pathElement)) {
            // don't append 
            return pathIn;
        }
        // double-check for duplicates
        if (pathIn.contains(pathElement)) {
            final String[] pathInElements=pathIn.split(File.pathSeparator);
            for(final String pathInElement : pathInElements) {
                if (pathInElement.equals(pathElement)) {
                    return pathIn;
                }
            }
        }
        return pathElement + File.pathSeparator + pathIn;
    }
    
    /**
     * Adds the specified path to the java library path
     *
     * @param pathToAdd the path to add
     * @throws Exception
     */
    public static void addLibraryPath(final String pathToAdd) throws Exception{
        final Field usrPathsField = ClassLoader.class.getDeclaredField("usr_paths");
        usrPathsField.setAccessible(true);

        //get array of paths
        final String[] paths = (String[])usrPathsField.get(null);

        //check if the path to add is already present
        for(String path : paths) {
            if(path.equals(pathToAdd)) {
                return;
            }
        }

        //add the new path
        final String[] newPaths = Arrays.copyOf(paths, paths.length + 1);
        newPaths[newPaths.length-1] = pathToAdd;
        usrPathsField.set(null, newPaths);
    }


}
