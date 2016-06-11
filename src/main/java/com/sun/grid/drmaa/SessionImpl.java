package com.sun.grid.drmaa;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.InvalidJobTemplateException;
import org.ggf.drmaa.JobInfo;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.Version;

/**
 * Hand-edited modification of the 'com.sun.grid.drmaa.SessionImpl' class included
 * with Univa Grid Engine v 8.3.0.
 * 
 * Runtime values:
 *     session.getVersion(): 1.0
 *     session.getDrmSystem(): UGE 8.3.0
 *     session.getDrmaaImplementation(): UGE 8.3.0
 * 
 * @author pcarr
 */
public class SessionImpl
implements Session
{
    private static final Logger log = Logger.getLogger(SessionImpl.class);

    //TODO: was 
    static {
        log.info("loading custom implementation of "+SessionImpl.class.getName());
//        AccessController.doPrivileged(new PrivilegedAction() {
//            public Object run() {
//                System.loadLibrary("drmaa");
//                return null;
//            }
//        });
    }

    @Override
    public void control(String jobId, int action)
    throws DrmaaException {
        nativeControl(jobId, action);
    }

    public void controlAs(AuthInfo ai, String jobId, int action)
    throws DrmaaException
    {
        nativeControlAs(ai, jobId, action);
    }

    @Override
    public void exit()
    throws DrmaaException
    {
        nativeExit();
    }

    @Override
    public String getContact()
    {
        return nativeGetContact();
    }

    @Override
    public String getDrmSystem()
    {
        // TODO: parameterize
        // was: return nativeGetDRMSInfo();
        return "UGE 8.3.0";
    }
    
    @Override
    public String getDrmaaImplementation()
    {
        return getDrmSystem();
    }
    
    @Override
    public int getJobProgramStatus(String jobId)
    throws DrmaaException
    {
        return nativeGetJobProgramStatus(jobId);
    }

    @Override
    public JobTemplate createJobTemplate()
    throws DrmaaException
    {
        int id = nativeAllocateJobTemplate();
        return new JobTemplateImpl(this, id);
    }
    
    @Override
    public void deleteJobTemplate(JobTemplate jt) throws DrmaaException
    {
        if (jt == null)
            throw new NullPointerException("JobTemplate is null");
        if ((jt instanceof JobTemplateImpl)) {
            nativeDeleteJobTemplate(((JobTemplateImpl)jt).getId());
        } 
        else {
            throw new InvalidJobTemplateException();
        }
    }

    @Override
    public Version getVersion()
    {
        return new Version(1, 0);
    }
  
    @Override
    public void init(String contact)
    throws DrmaaException
    {
        //TODO: set SGE_ flags here
        nativeInit(contact);
    }
  
    @Override
    public List runBulkJobs(JobTemplate jt, int start, int end, int incr) throws DrmaaException
    {
        if (jt == null)
            throw new NullPointerException("JobTemplate is null");
        if ((jt instanceof JobTemplateImpl)) {
            String[] jobIds = nativeRunBulkJobs(((JobTemplateImpl)jt).getId(), start, end, incr);
            return Arrays.asList(jobIds);
        }
        throw new InvalidJobTemplateException();
    }

  public List runBulkJobsAs(AuthInfo ai, JobTemplate jt, int start, int end, int incr)
    throws DrmaaException
  {
    if (jt == null)
      throw new NullPointerException("JobTemplate is null");
    if ((jt instanceof JobTemplateImpl)) {
      String[] jobIds = nativeRunBulkJobsAs(ai, ((JobTemplateImpl)jt).getId(), start, end, incr);
      


      return Arrays.asList(jobIds);
    }
    throw new InvalidJobTemplateException();
  }

    @Override
    public String runJob(JobTemplate jt)
    throws DrmaaException
    {
        if (jt == null)
            throw new NullPointerException("JobTemplate is null");
        if ((jt instanceof JobTemplateImpl)) {
            return nativeRunJob(((JobTemplateImpl)jt).getId());
        }
        throw new InvalidJobTemplateException();
    }

  public String runJobAs(AuthInfo ai, JobTemplate jt)
    throws DrmaaException
  {
    if (jt == null)
      throw new NullPointerException("JobTemplate is null");
    if ((jt instanceof JobTemplateImpl)) {
      return nativeRunJobAs(ai, ((JobTemplateImpl)jt).getId());
    }
    throw new InvalidJobTemplateException();
  }
  

  @Override
  public void synchronize(List jobIds, long timeout, boolean dispose)
    throws DrmaaException
  {
    nativeSynchronize((String[])jobIds.toArray(new String[jobIds.size()]), timeout, dispose);
  }
  
  @Override
  public JobInfo wait(String jobId, long timeout)
    throws DrmaaException
  {
    JobInfoImpl jobInfo = nativeWait(jobId, timeout);
    
    return jobInfo;
  }
  
  private native void nativeControl(String paramString, int paramInt)
    throws DrmaaException;
  
  private native void nativeControlAs(AuthInfo paramAuthInfo, String paramString, int paramInt)
    throws DrmaaException;
  
  private native void nativeExit()
    throws DrmaaException;
  
  private native String nativeGetContact();
  
  private native String nativeGetDRMSInfo();
  
  private native int nativeGetJobProgramStatus(String paramString)
    throws DrmaaException;
  
  private native void nativeInit(String paramString)
    throws DrmaaException;
  
  private native String[] nativeRunBulkJobs(int paramInt1, int paramInt2, int paramInt3, int paramInt4)
    throws DrmaaException;
  
  private native String[] nativeRunBulkJobsAs(AuthInfo paramAuthInfo, int paramInt1, int paramInt2, int paramInt3, int paramInt4)
    throws DrmaaException;
  
  private native String nativeRunJob(int paramInt)
    throws DrmaaException;
  
  private native String nativeRunJobAs(AuthInfo paramAuthInfo, int paramInt)
    throws DrmaaException;
  
  private native void nativeSynchronize(String[] paramArrayOfString, long paramLong, boolean paramBoolean)
    throws DrmaaException;
  
  private native JobInfoImpl nativeWait(String paramString, long paramLong)
    throws DrmaaException;
  
  private native int nativeAllocateJobTemplate()
    throws DrmaaException;
  
  native void nativeSetAttributeValue(int paramInt, String paramString1, String paramString2)
    throws DrmaaException;
  
  native void nativeSetAttributeValues(int paramInt, String paramString, String[] paramArrayOfString)
    throws DrmaaException;
  
  native String[] nativeGetAttributeNames(int paramInt)
    throws DrmaaException;
  
  native String[] nativeGetAttribute(int paramInt, String paramString)
    throws DrmaaException;
  
  native void nativeDeleteJobTemplate(int paramInt)
    throws DrmaaException;
}
