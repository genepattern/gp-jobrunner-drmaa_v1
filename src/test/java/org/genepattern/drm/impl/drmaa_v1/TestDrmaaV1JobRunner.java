package org.genepattern.drm.impl.drmaa_v1;

import static org.mockito.Mockito.mock;

import org.genepattern.drm.DrmJobRecord;
import org.genepattern.drm.DrmJobStatus;
import org.genepattern.drm.JobRunner;
import org.junit.Test;

public class TestDrmaaV1JobRunner {
    @Test
    public void runTest() {
        DrmaaV1JobRunner jobRunner=new DrmaaV1JobRunner();
        DrmJobRecord jobRecord=mock(DrmJobRecord.class);
        DrmJobStatus jobStatus=jobRunner.getStatus(jobRecord);
    }
}
