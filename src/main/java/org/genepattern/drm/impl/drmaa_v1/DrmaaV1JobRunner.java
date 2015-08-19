package org.genepattern.drm.impl.drmaa_v1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.genepattern.drm.DrmJobRecord;
import org.genepattern.drm.DrmJobState;
import org.genepattern.drm.DrmJobStatus;
import org.genepattern.drm.DrmJobSubmission;
import org.genepattern.drm.JobRunner;
import org.genepattern.server.executor.CommandExecutorException;
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

/**
 * JobRunner for GridEngine integration with DRMAA v1 compliant library.
 * @author pcarr
 *
 */
public class DrmaaV1JobRunner implements JobRunner {
    private static final Logger log = Logger.getLogger(DrmaaV1JobRunner.class);

    private Session session=null;
    private DrmaaException sessionInitError=null;
    
    private static final Map<Integer, DrmJobState> map;
    static {
        map=new HashMap<Integer, DrmJobState>();
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
        final Session session=getSession();
        final String cmd=jobSubmission.getCommandLine().get(0);
        final List<String> args=jobSubmission.getCommandLine().subList(1, jobSubmission.getCommandLine().size());
        try {
            final String jobId=submitJob(session, cmd, args);
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
        catch (CommandExecutorException e) {
            return new DrmJobStatus.Builder()
                .extJobId(drmJobRecord.getExtJobId())
                .jobState(DrmJobState.UNDETERMINED)
                .jobStatusMessage("job queue error: "+e.getLocalizedMessage())
            .build();
        }
        catch (DrmaaException e) {
            return new DrmJobStatus.Builder()
                .extJobId(drmJobRecord.getExtJobId())
                .jobState(DrmJobState.UNDETERMINED)
                .jobStatusMessage("job queue error: "+e.getLocalizedMessage())
            .build();
        }
    }

    @Override
    public boolean cancelJob(DrmJobRecord drmJobRecord) throws Exception {
        final Session session=getSession();
        return requestCancelJob(session, drmJobRecord.getExtJobId());
    }
    
    protected Session initSession() throws DrmaaException {
        Session session=SessionFactory.getFactory().getSession();
        log.info("initializing session...");
        log.info("\tversion: "+session.getVersion());
        log.info("\tdrmSystem: "+session.getDrmSystem());
        log.info("\tdrmaaImplementation: "+ session.getDrmaaImplementation());
        session.init(null);
        log.info("Done!");
        return session;
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

    protected String submitJob(final Session session, final String cmd, final List<String> args) throws DrmaaException {
        JobTemplate jobTemplate = session.createJobTemplate();
        jobTemplate.setRemoteCommand(cmd);
        jobTemplate.setArgs(args);
        String jobId=session.runJob(jobTemplate);
        return jobId;
    }
    
    protected DrmJobStatus requestStatus(final Session session, final String extJobId) throws DrmaaException {
        if (log.isDebugEnabled()) {
            log.debug("requesting status, jobId="+extJobId);
        }
        JobInfo jobInfo=null;
        try {
            final long timeout_seconds=5;
            jobInfo=session.wait(extJobId, timeout_seconds);
            log.debug("wait completed!, jobId="+extJobId);
            
            DrmJobStatus.Builder b=new DrmJobStatus.Builder()
                .extJobId(jobInfo.getJobId());
            
            if (jobInfo.wasAborted()) {
                log.debug("wasAborted");
                b.jobState(DrmJobState.ABORTED);
            }
            else if (jobInfo.hasExited()) {
                log.debug("hasExited");
                b.exitCode(jobInfo.getExitStatus());
                if (jobInfo.getExitStatus()==0) {
                    b.jobState(DrmJobState.DONE);
                }
                else {
                    b.jobState(DrmJobState.FAILED);
                }
            }
            else if (jobInfo.hasSignaled()) {
                log.debug("hasSignaled");
                b.jobState(DrmJobState.FAILED);
                b.jobStatusMessage("drmaa: jobInfo.hasSignaled()==true");
            }
            else if (jobInfo.hasCoreDump()) {
                log.debug("hasCoreDump");
                b.jobState(DrmJobState.FAILED);
                b.jobStatusMessage("drmaa: jobInfo.hasCoreDump()==true");
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
        
        final int drmaaStatusId=session.getJobProgramStatus(extJobId);
        
        DrmJobState gpState=map.get(drmaaStatusId);
        if (gpState==null) {
            gpState=DrmJobState.UNDETERMINED;
        }
        
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
