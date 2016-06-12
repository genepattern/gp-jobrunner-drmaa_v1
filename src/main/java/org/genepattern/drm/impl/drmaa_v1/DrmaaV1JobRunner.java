package org.genepattern.drm.impl.drmaa_v1;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.genepattern.drm.CpuTime;
import org.genepattern.drm.DrmJobRecord;
import org.genepattern.drm.DrmJobState;
import org.genepattern.drm.DrmJobStatus;
import org.genepattern.drm.DrmJobSubmission;
import org.genepattern.drm.JobRunner;
import org.genepattern.drm.Memory;
import org.genepattern.drm.util.Env;
import org.genepattern.server.config.Value;
import org.genepattern.server.executor.CommandExecutorException;
import org.genepattern.server.executor.CommandProperties;
import org.ggf.drmaa.AuthorizationException;
import org.ggf.drmaa.DrmCommunicationException;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.ExitTimeoutException;
import org.ggf.drmaa.InternalException;
import org.ggf.drmaa.InvalidJobException;
import org.ggf.drmaa.JobInfo;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.NoActiveSessionException;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.primitives.Doubles;

/**
 * JobRunner for GridEngine integration with DRMAA v1 library.
 * 
 * <h1>Configuration guide</h1>
 * 
 * Example config_yaml entry:
<pre>
executors:
    #
    # JobRunner API integration for Univa Grid Engine with DRMAA v1
    #
    UGER:
        classname: org.genepattern.server.executor.drm.JobExecutor
        configuration.properties:
            jobRunnerClassname: org.genepattern.drm.impl.drmaa_v1.DrmaaV1JobRunner
            jobRunnerName: UGER
        default.properties:
            job.queue: short

            ### 
            # UGER specific flags
            ###
            
            # [optional] set the Parallel Execution environment for a multi-core job
            # 
            # This flag is used when the job.cpuCount or job.nodeCount > 1.
            #     -pe <job.gp.pe_type> <job.cpuCount>
            # Use the 'qconf -spl' command to list available parallel environments. Broad hosted UGER options are:
            #     mpi | openmpi | smp
            #
            # To learn more about a specific PE, type something like:
            #     qconf -sp openmpi
            #
            job.ge.pe_type: smp

            # Example configuration (still under construction) for Broad UGER cluster
            # Set system environment after JVM startup (instead via 'use UGER')
            # 
            # See: https://blogs.oracle.com/templedf/entry/drmaa_and_the_shared_library
            #
 
            DRMAA_LIBRARY_PATH: [ "/xchip/gpdev/servers/gp-trunk/gp/gputiljni/libgputil.so" ]
            # UGER system environment variables
            LD_LIBRARY_PATH: [ "/broad/uge/research/lib/lx-amd64" ]
            SGE_ENV: {
                "SGE_ROOT":         "/broad/uge/research",
                "SGE_CELL":         "research",
                "SGE_CLUSTER_NAME": "uger",
                "SGE_EXECD_PORT":   "6445",
                "SGE_QMASTER_PORT": "6444",
            }
 * </pre>
 * 
 * <h1>Debugging Guide</h1>
 * Example GridEngine command lines.
 * 
 * 
 * @author pcarr
 *
 */
public class DrmaaV1JobRunner implements JobRunner {
    private static final Logger log = Logger.getLogger(DrmaaV1JobRunner.class);

    /**
     * Set the 'job.ge.clear' boolean flag to clear default settings as the first arg of the 
     * native specification, e.g.
     *     job.ge.clear: true
     *  
     */
    public static final String PROP_CLEAR="job.ge.clear";
    
    /**
     * Set the 'job.ge.pe_type' to specify the parallel environment for a multi-core job
     */
    public static final String PROP_PE_TYPE="job.ge.pe_type";

    /**
     * Set the 'DRMAA_LIBRARY_PATH' to the full path to the drmaa library. 
     *     System.load(<DRMAA_LIBRARY_PATH>);
     * 
     */
    public static final String PROP_DRMAA_LIBRARY_PATH="DRMAA_LIBRARY_PATH";
 
    /**
     * lookup table for selecting an entry from the GenePattern DrmJobState enum 
     * from a DRMAA v1 jobProgramStatus flag
     * 
     * @see Session#
     */
    public static final Map<Integer, DrmJobState> jobStateMap;
    static {
        Map<Integer, DrmJobState> map=new HashMap<Integer, DrmJobState>();
        map.put(Session.UNDETERMINED, DrmJobState.UNDETERMINED);
        map.put(Session.QUEUED_ACTIVE, DrmJobState.QUEUED);
        map.put(Session.SYSTEM_ON_HOLD, DrmJobState.QUEUED_HELD);
        map.put(Session.USER_ON_HOLD, DrmJobState.QUEUED_HELD);
        map.put(Session.RUNNING, DrmJobState.RUNNING);
        map.put(Session.SYSTEM_SUSPENDED, DrmJobState.SUSPENDED);
        map.put(Session.USER_SUSPENDED, DrmJobState.SUSPENDED);
        map.put(Session.USER_SYSTEM_SUSPENDED, DrmJobState.SUSPENDED);
        map.put(Session.DONE, DrmJobState.DONE);
        map.put(Session.FAILED, DrmJobState.FAILED);        
        jobStateMap = Collections.unmodifiableMap(map);
    }
    
    private Session session=null;
    private DrmaaException sessionInitError=null;
    private Value ld_library_path=null;
    private Value drmaaLibPath=null;
    private Value sgeEnv=null;

    public void setCommandProperties(final CommandProperties properties) {
        // /broad/uge/8.3.1p6/lib/lx-amd64        
        this.ld_library_path=properties.get("LD_LIBRARY_PATH");
        // optionally load the library path
        this.drmaaLibPath=properties.get(PROP_DRMAA_LIBRARY_PATH);
        // optionally set environment variables
        this.sgeEnv=properties.get("SGE_ENV");
    }

    protected String concatProps(final String prop, final String props) {
        if (props==null || props.length()==0) {
            return prop;
        }
        else {
            return prop + System.getProperty("path.separator") + props;
        }
    }

    public void start() { 
        try {
            this.sessionInitError=null;
            this.session=initSession();
        }
        catch (final DrmaaException e) {
            log.error("Error initializing session on startup", e);
            this.sessionInitError=e;
        }
        catch (final Throwable t) {
            log.error("Unexpected error initializing session on startup", t);
        }
    }
    
    @Override
    public void stop() {
        // TODO: not thread safe
        if (session != null) {
            try {
                session.exit();
            }
            catch (DrmaaException e) {
                log.error("Error in session.exit()", e);
            }
        } 
        this.session=null;
        this.sessionInitError=null;
    }

    @Override
    public String startJob(final DrmJobSubmission jobSubmission) throws CommandExecutorException {
        validateCmdLine(jobSubmission);
        Util.logCommandLine(jobSubmission);
        final Session session=getSession();
        try {
            final String jobId=submitJob(session, jobSubmission);
            return jobId;
        }
        catch (DrmaaException e) {
            final String msg="Error adding job to queue, gpJobNo="+jobSubmission.getGpJobNo()+", DrmaaException="+e.getLocalizedMessage();
            log.debug(msg, e);
            throw new CommandExecutorException(msg, e);
        }
        catch (Throwable t) {
            final String msg="Unexpected exception adding job to queue, gpJobNo="+jobSubmission.getGpJobNo()+": "+t.getLocalizedMessage();
            log.error(msg, t);
            throw new CommandExecutorException(msg, t);
        }
    }

    @Override
    public DrmJobStatus getStatus(DrmJobRecord drmJobRecord) {
        try {
            final Session session = getSession();
            return requestStatus(session, drmJobRecord.getExtJobId());
        }
        // TODO: deal with these exceptions, UNDETERMINED causes the job to be flagged as cancelled in GP
        catch (CommandExecutorException e) {
            log.error("Error getting status for gpJobNo="+drmJobRecord.getGpJobNo(), e);
            return new DrmJobStatus.Builder()
                .extJobId(drmJobRecord.getExtJobId())
                .jobState(DrmJobState.UNDETERMINED)
                .jobStatusMessage("job queue error: "+e.getLocalizedMessage())
            .build();
        }
        catch (DrmaaException e) {
            log.error("Error getting status for gpJobNo="+drmJobRecord.getGpJobNo(), e);
            return new DrmJobStatus.Builder()
                .extJobId(drmJobRecord.getExtJobId())
                .jobState(DrmJobState.UNDETERMINED)
                .jobStatusMessage("job queue error: "+e.getLocalizedMessage())
            .build();
        }
        catch (Throwable t) {
            log.error("Error getting status for gpJobNo="+drmJobRecord.getGpJobNo(), t);
            return new DrmJobStatus.Builder()
                .extJobId(drmJobRecord.getExtJobId())
                .jobState(DrmJobState.UNDETERMINED)
                .jobStatusMessage("job queue error: "+t.getLocalizedMessage())
            .build();
        }
    }

    @Override
    public boolean cancelJob(DrmJobRecord drmJobRecord) throws Exception {
        final Session session=getSession();
        return requestCancelJob(session, drmJobRecord.getExtJobId());
    }
    
    /**
     * Initialize the DRMAA session based on the system environment
     * when starting the server. For example,
     *     use '.uges-8.3.1p6'
     *     nohup ./StartGenePatternServer &
     * 
     * Environment for Broad hosted UGES cluster:
     *     DRMAA_LIBRARY_PATH=/broad/uge/8.3.1p6/lib/lx-amd64/libdrmaa.so
     *     LD_LIBRARY_PATH=/broad/uge/8.3.1p6/lib/lx-amd64
     *     PATH=/broad/uge/8.3.1p6/bin/lx-amd64
     *     SGE_ROOT=/broad/uge/8.3.1p6
     *     SGE_CELL=service
     *     SGE_CLUSTER_NAME=uges-831p6
     *     SGE_EXECD_PORT=6445
     *     SGE_QMASTER_PORT=6444
     * 
     * For multi-cluster systems, the environment variables must be set at runtime 
     * after the startup of the JVM. These are set as a map in the config_yaml file.
     *     SGE_ENV: {
     *         SGE_ROOT: "/path/to/ge",
     *         ...
     *     }
     * 
     * @return
     * @throws DrmaaException
     */
    protected Session initSession() throws DrmaaException {
        // optionally modify the LD_LIBRARY_PATH ...
        if (ld_library_path != null) {
            for(final String path : ld_library_path.getValues()) {
                final String message="adding path to LD_LIBRARY_PATH, path="+path;
                try {
                    log.info(message); 
                    addLibraryPath(path);
                }
                catch (Throwable t) {
                    log.error("Error "+message, t);
                }
            }
        }
        
        // optionally load shared libraries ... 
        if (drmaaLibPath != null) {
            for(final String library : drmaaLibPath.getValues()) {
                try {
                    log.info("loading library '"+library+"' ...");
                    System.load(library);
                }
                catch (Throwable t) {
                    log.error("Error in System.load('"+library+"')", t);
                }
            }
        }

        // optionally set system environment variables ...
        if (sgeEnv != null && sgeEnv.isMap()) {
            final Env env=new Env();
            for(final Entry<?,?> entry : sgeEnv.getMap().entrySet()) {
                final String name= (String) entry.getKey();
                final String value= (String) entry.getValue();
                log.info("setting system environment "+name+"="+value);
                env.setEnvironmentVariable(name, value);
            }
        }
        
        log.info("initializing session...");
        final Session session=SessionFactory.getFactory().getSession();
        log.info("\tversion: "+session.getVersion());
        log.info("\tdrmSystem: "+session.getDrmSystem());
        log.info("\tdrmaaImplementation: "+ session.getDrmaaImplementation());
        
        session.init(null);
        log.info("Done!");
        if (log.isDebugEnabled()) {
            debugInitTemplate(session);
        }
        return session;
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

    protected Session getSession() throws CommandExecutorException {
        if (this.session==null) {
            if (this.sessionInitError != null) {
                throw new CommandExecutorException("DRMAA configuration error", sessionInitError);
            }
            else {
                throw new CommandExecutorException("session is not initialized");
            }
        }
        return session;
    }
    
    protected String initFilePath(final DrmJobSubmission jobSubmission, final String name) {
        return new File(jobSubmission.getWorkingDir(), name).getAbsolutePath();
    }
    
    protected static void debugInitTemplate(final Session session) {
        if (session==null) {
            log.error("session==null");
            return;
        }
        JobTemplate jt = null;
        try {
            log.debug("creating JobTemplate for debugging...");
            jt = session.createJobTemplate();
            log.debug("getAttributeNames() ...");
            final Set<?> attNames=jt.getAttributeNames();
            if (attNames != null) {
                for(final Object attName : attNames) {
                    log.debug("\t"+attName.toString());
                }
            }
        }
        catch (DrmaaException e) {
            log.error("caught DrmaaException: "+e.getLocalizedMessage(), e);
        }
        catch (Throwable t) {
            log.error("caught Unexpected Error: "+t.getLocalizedMessage(), t);
        }
        finally {
            if (jt != null) {
                try {
                    session.deleteJobTemplate(jt);
                }
                catch (Throwable t) {
                    log.error("Error in session.deleteJobTemplate", t);
                }
            }
        }
    }
    
    protected void validateCmdLine(final DrmJobSubmission jobSubmission) throws CommandExecutorException {
        if (jobSubmission.getCommandLine()==null) {
            throw new CommandExecutorException("jobSubmission.commandLine==null");
        }
        else if (jobSubmission.getCommandLine().size()==0) {
            throw new CommandExecutorException("jobSubmission.commandLine.size==0");
        }
    }
    
    protected boolean isClear(final DrmJobSubmission jobSubmission) {
        if (jobSubmission == null || jobSubmission.getGpConfig() == null) {
            return false;
        }
        return jobSubmission.getGpConfig().getGPBooleanProperty(jobSubmission.getJobContext(), PROP_CLEAR);
    }
    
    /**
     * @see https://blogs.oracle.com/templedf/entry/using_drm_specific_functionality_via
     * @return
     */
    protected List<String> initNativeSpecification(final DrmJobSubmission jobSubmission) {
        final List<String> rval=new ArrayList<String>();
        
        // optional put the '-clear' flag at the start of the spec
        if (isClear(jobSubmission)) {
            rval.add("-clear");
        }
        
        // always use the '-o' flag
        final String stdout=initFilepath(jobSubmission.getWorkingDir(), jobSubmission.getStdoutFile(), "stdout.txt");
        rval.add("-o");
        rval.add(stdout);

        // always us the '-e' flag
        final String stderr=initFilepath(jobSubmission.getWorkingDir(), jobSubmission.getStderrFile(), "stderr.txt");
        rval.add("-e");
        rval.add(stderr);

        // optionally use the '-i' flag
        if (jobSubmission.getStdinFile() != null) {
            final String stdin=initFilepath(jobSubmission.getWorkingDir(), jobSubmission.getStdinFile(), "stdin.txt");
            rval.add("-i");
            rval.add(stdin);
        }
        
        // optionally set the memory flag
        if (jobSubmission.getMemory() != null) {
            //-l m_mem_free=Xg
            rval.add("-l");
            rval.add("m_mem_free="+((long)Math.ceil(jobSubmission.getMemory().numGb()))+"g");
        }
        
        // optionally set the queue flag
        if (!Strings.isNullOrEmpty(jobSubmission.getQueue())) {
            rval.add("-q");
            rval.add(jobSubmission.getQueue());
        }
        
        // optionally set the project flag
        final String project=jobSubmission.getProperty(JobRunner.PROP_PROJECT);
        if (!Strings.isNullOrEmpty(project)) {
            rval.add("-P");
            rval.add(project);
        }
        
        // optionally set the '-pe' flag, for parallel jobs
        rval.addAll(getPeFlags(jobSubmission));
        
        // optionally add any extra args
        final List<String> extraArgs=jobSubmission.getExtraArgs();
        if (extraArgs != null) { 
            rval.addAll(extraArgs);
        }
        return rval;
    }

    /**
     * Helper method to deal with two possible flags:
     *     job.cpuCount and job.nodeCount
     *     
     * @param jobSubmission
     * @return
     */
    protected Integer getNumCores(final DrmJobSubmission jobSubmission) {
        Integer nodeCount=jobSubmission.getNodeCount();
        Integer cpuCount=jobSubmission.getCpuCount();
        if (cpuCount==nodeCount) return cpuCount;
        if (nodeCount==null && cpuCount !=null) return cpuCount;
        if (cpuCount==null && nodeCount !=null) return nodeCount;
        
        // special-case: both set, but they differ        
        return Math.max(nodeCount, cpuCount); 
    }
    
    protected String getPeType(final DrmJobSubmission jobSubmission) {
        String peType=jobSubmission.getProperty(PROP_PE_TYPE);
        if (peType==null) {
            peType="smp"; // default value
        }
        return peType;
    }
    
    /**
     * Get the [optional] parallel environment flags as nativeSpec args based on the
     * <job.cpuCount> or <job.nodeCount> and the [optional] <job.ge.pe_type>.
     * 
     * Example qsub command-line:
     *     qsub -pe openmpi 4 -b y my_wonderful_multiprocessor_app
     * 
     */
    protected List<String> getPeFlags(final DrmJobSubmission jobSubmission) {
        final Integer numCores=getNumCores(jobSubmission);
        if (numCores==null) {
            return Collections.emptyList();
        }
        else if (numCores <= 1) {
            log.warn("numCores="+numCores+", ignore");
            return Collections.emptyList();
        }
        
        final String peType=getPeType(jobSubmission);
        return Arrays.asList("-pe", peType, ""+numCores);
    }
    
    protected String initFilepath(final File workingDir, final File ioFile, final String defaultValue) {
        String filepath=defaultValue;
        if (ioFile != null) {
            filepath=ioFile.getPath();
            // special-case: if it's in the working directory
            if (ioFile.isAbsolute()) {
                if (ioFile.getParentFile() != null) {
                    if (ioFile.getParentFile().equals( workingDir )) {
                        filepath=ioFile.getName();
                    }
                }
            }
        }
        return filepath;
    }
    
    protected String formatNativeSpecification(final List<String> args) {
        Joiner joiner=Joiner.on(' ').useForNull("");
        return joiner.join(args);
    }

    /**
     * Create a new JobTemplate for submitting a job.
     * @param session
     * @param jobSubmission with a valid commandLine, Hint: must call validateCmdLine before calling this
     * @return
     * @throws DrmaaException
     */
    protected JobTemplate initJobTemplate(final Session session, final DrmJobSubmission jobSubmission) throws DrmaaException {
        JobTemplate jt = session.createJobTemplate();
        jt.setJobName("GP_"+jobSubmission.getGpJobNo());
        jt.setWorkingDirectory(jobSubmission.getWorkingDir().getAbsolutePath());
        jt.setJoinFiles(false);
        List<String> nativeSpecArgs=initNativeSpecification(jobSubmission);
        final String nativeSpec=formatNativeSpecification(nativeSpecArgs);
        jt.setNativeSpecification(nativeSpec);
        
        final String cmd;
        final List<String> args;
        cmd=jobSubmission.getCommandLine().get(0);
        args=jobSubmission.getCommandLine().subList(1, jobSubmission.getCommandLine().size());
        jt.setRemoteCommand(cmd);
        jt.setArgs(args);

        return jt;
    }
    
    protected String submitJob(final Session session, final DrmJobSubmission job) throws DrmaaException {
        JobTemplate jt=initJobTemplate(session, job);        
        String jobId=session.runJob(jt);
        session.deleteJobTemplate(jt);
        return jobId;
    }
    
    protected DrmJobState requestDrmJobState(final Session session, final String extJobId) throws DrmaaException {
        if (log.isDebugEnabled()) {
            log.debug("getJobProgramStatus("+extJobId+")");
        }
        final int drmaaStatusId=session.getJobProgramStatus(extJobId);
        DrmJobState gpState=jobStateMap.get(drmaaStatusId);
        if (gpState==null) {
            gpState=DrmJobState.UNDETERMINED;
        }
        return gpState;
    }
    
    /**
     * Parse the "cpu" from the usage map, e.g.
     *     # The cpu time usage in seconds.
     *     cpu=2720.2300
     * @param cpuStr
     * @return
     */
    protected static CpuTime asCpuTime(final String cpuStr) {
        if (Strings.isNullOrEmpty(cpuStr)) {
            log.debug("cpuStr isNullOrEmpty");
            return null;
        }
        final Double d=Doubles.tryParse(cpuStr);
        if (d==null) {
            log.error("Expecting a valid Double, cpuStr="+cpuStr);
            return null;
        }
        final CpuTime cpuTime=new CpuTime(Math.round(d.doubleValue()*1000.0));
        return cpuTime;
    }
    
    protected static Date parseDate(final String dateStr) {
        if (Strings.isNullOrEmpty(dateStr)) {
            log.debug("dateStr isNullOrEmpty");
            return null;
        }
        final Double d=Doubles.tryParse(dateStr);
        if (d==null) {
            log.error("Expecting a valid number, dateStr="+dateStr);
            return null;
        }
        
        return new Date((long)Math.floor(d));
    }
    
    /**
     * Parse a memory value from an entry in the resourceUsage map.
     * For example:
     *     # without units, assume bytes (can be decimal or integer value)
     *     maxvmem=1473015808.0000
     *     maxvmem=1473015808
     *     
     *     # with units, parse with Memory.fromString
     *     maxvmem=1015.188M
     *     maxvmem=1.050G
     *   
     * @param memStr
     * @return
     */
    protected static Memory parseMemory(final String memStr) {
        if (Strings.isNullOrEmpty(memStr)) {
            log.debug("memStr isNullOrEmpty");
            return null;
        }
        final Double d=Doubles.tryParse(memStr);
        if (d != null) {
            if (log.isDebugEnabled()) {
                log.debug("no units, assume bytes, memStr="+memStr);
            }
            return Memory.fromSizeInBytes((long)Math.floor(d));
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug("calling Memory.fromString("+memStr+")");
            }
            return Memory.fromString(memStr);
        }
        catch (Throwable t) {
            log.error("Expecting a valid memory spec, memStr="+memStr);
            return null;
        }
    }

    /**
     * Append usage stats for a completed job, based on the given DRMAA JobInfo object.
     * 
     * See: man accounting(5) for details
     * See: man getrusage(2) for details
     * 
     * 
     * Example output:
<pre>
recording status for gpJobNo=79917, updatedJobStatus=drmJobId=169437, queueId=null, jobState=RUNNING, exitCode=null
# The cpu time usage in seconds.
 - cpu=2720.2300
# The maximum vmem size in bytes. 
 - maxvmem=1473015808.0000
# The wallclock time the job spent in running state.
 - wallclock=823.5770
# The integral memory usage in Gbytes cpu seconds.
 - mem=1283.4693
#  64bit GMT unix time stamp in milliseconds
 - start_time=1440658419871.0000
 - submission_time=1440658209738.0000

 - acct_cpu=2720.2300
 - acct_io=27.9707
 - acct_iow=0.0000
 - acct_maxvmem=1473015808.0000
 - acct_mem=1283.4693
 - end_time=1440659243171.0000
 - exit_status=0.0000
 - io=27.9707
 - iow=0.0000
 - priority=0.0000

 - ru_idrss=0.0000
 - ru_inblock=64.0000
 - ru_ismrss=0.0000
 - ru_isrss=0.0000
 - ru_ixrss=0.0000
 - ru_majflt=0.0000
 - ru_maxrss=18024.0000
 - ru_minflt=9701.0000
 - ru_msgrcv=0.0000
 - ru_msgsnd=0.0000
 - ru_nivcsw=4.0000
 - ru_nsignals=0.0000
 - ru_nswap=0.0000
 - ru_nvcsw=553.0000
 - ru_oublock=104.0000
 - ru_stime=0.0610
 - ru_utime=0.3289
 - ru_wallclock=823.3000

 - signal=0.0000
 - vmem=0.0000


</pre>
     * 
     * @param b
     * @param jobInfo
     * @throws DrmaaException
     */
    
    protected Map<String,String> initResourceUsageMap(final JobInfo jobInfo) {
        Map<?,?> usageIn=null;
        try {
            usageIn=jobInfo.getResourceUsage();
        }
        catch (Throwable t) {
            log.error("Error getting resourceUsage from DRMAA jobInfo: "+t.getLocalizedMessage(), t);
        }
        if (usageIn == null || usageIn.isEmpty()) {
            return Collections.emptyMap();
        }
        SortedMap<String,String> usage=new TreeMap<String,String>();
        if (log.isDebugEnabled()) {
            log.debug("jobInfo.resourceUsage ...");
        }
        for(final Entry<?,?> e : usageIn.entrySet()) {
            final String key=e.getKey().toString();
            final String val= e.getValue() == null ? "" : e.getValue().toString();
            if (log.isDebugEnabled()) {
                log.debug(key+"="+val);
            }
            usage.put(key, val);
        }
        return usage;
    }
    
    protected void logUsageStats(final DrmJobStatus.Builder b, final JobInfo jobInfo) {
        Map<String,String> usage=initResourceUsageMap(jobInfo);
        logUsageStats(b, usage);
    }

    protected void logUsageStats(final DrmJobStatus.Builder b, final Map<String,String> usage) {
        b.resourceUsage(usage);
        
        // The cpu time usage in seconds, e.g. cpu=2720.2300
        if (usage.containsKey("cpu")) {
            final CpuTime cpuTime=asCpuTime(usage.get("cpu"));
            b.cpuTime(cpuTime);
        }
        
        // start_time=1440658419871.0000
        if (usage.containsKey("start_time")) {
            final Date startTime=parseDate(usage.get("start_time"));
            b.startTime(startTime);
        }
        
        // submission_time=1440658209738.0000
        if (usage.containsKey("submission_time")) {
            final Date submitTime=parseDate(usage.get("submission_time"));
            b.submitTime(submitTime);
        }
        
        // # The maximum vmem size in bytes. 
        // maxvmem=1473015808.0000;
        if (usage.containsKey("maxvmem")) {
            Memory maxVmem=parseMemory(usage.get("maxvmem"));
            if (maxVmem != null) {
                b.memory(maxVmem);
            }
        }

        // TODO:
        //b.maxProcesses(maxProcesses);
        //b.maxSwap(maxSwap);
        //b.maxThreads(maxThreads);
    }
    
    protected DrmJobStatus requestStatus(final Session session, final String extJobId) throws DrmaaException {
        if (log.isDebugEnabled()) {
            log.debug("requesting status, jobId="+extJobId);
        }
        JobInfo jobInfo=null;
        try {
            final long timeout_seconds=5;
            jobInfo=session.wait(extJobId, timeout_seconds);
            log.debug("wait completed!, extJobId="+extJobId);
            
            DrmJobStatus.Builder b=new DrmJobStatus.Builder()
                .extJobId(jobInfo.getJobId());
            
            logUsageStats(b, jobInfo);
            
            if (jobInfo.hasExited()) {
                log.debug("hasExited, exitStatus="+jobInfo.getExitStatus());
                b.exitCode(jobInfo.getExitStatus());
                if (jobInfo.getExitStatus()==0) {
                    b.jobState(DrmJobState.DONE);
                }
                else {
                    b.jobState(DrmJobState.FAILED);
                }
            }
            else if (jobInfo.wasAborted()) {
                log.debug("wasAborted");
                log.debug("jobInfo="+jobInfo);
                b.jobState(DrmJobState.ABORTED);
            }
            else if (jobInfo.hasSignaled()) {
                final String msg="hasSignaled, terminatingSignal="+jobInfo.getTerminatingSignal();
                log.debug(msg);
                b.jobState(DrmJobState.FAILED);
                b.jobStatusMessage(msg);
                b.terminatingSignal(jobInfo.getTerminatingSignal());
            }
            else if (jobInfo.hasCoreDump()) {
                log.debug("hasCoreDump");
                b.jobState(DrmJobState.FAILED);
                b.jobStatusMessage("hasCoreDump");
            }
            else {
                log.debug("finished with unclear conditions");
                // finished with unclear conditions
                b.jobState(DrmJobState.UNDETERMINED);
                b.jobStatusMessage("finished with unclear conditions");
            } 
            return b.build();
        }
        catch (ExitTimeoutException e) {
            log.debug("presumably still running", e);
        }
        catch (InvalidJobException e) {
            log.error(e);
            throw e;
        }
        catch (NoActiveSessionException e) {
            log.error(e);
            throw e;
        }
        catch (DrmCommunicationException e) {
            log.error(e);
            throw e;
        }
        catch (AuthorizationException e) {
            log.error(e);
            throw e;
        }
        catch (IllegalArgumentException e) {
            log.error(e);
            throw e;
        }
        catch (InternalException e) {
            log.error(e);
            throw e;
        }
        catch (Throwable t) {
            log.error(t);
        }

        if (log.isDebugEnabled()) {
            log.debug("job not finished, extJobId="+extJobId);
        }
        final DrmJobState gpState=requestDrmJobState(session, extJobId);
        return new DrmJobStatus.Builder()
            .extJobId(extJobId)
            .jobState(gpState)
        .build();
    }
    
    protected boolean requestCancelJob(final Session session, final String extJobId) throws DrmaaException {
        session.control(extJobId, Session.TERMINATE);
        return true;
    }

}
