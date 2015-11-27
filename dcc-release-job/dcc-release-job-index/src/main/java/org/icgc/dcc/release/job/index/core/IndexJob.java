package org.icgc.dcc.release.job.index.core;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.icgc.dcc.release.core.job.GenericJob;
import org.icgc.dcc.release.core.job.JobContext;
import org.icgc.dcc.release.core.job.JobType;
import org.icgc.dcc.release.job.index.config.IndexProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor(onConstructor = @__({ @Autowired }))
public class IndexJob extends GenericJob {

	/**
	 * Dependencies.
	 */
	@NonNull
	private final IndexProperties properties;

	static String resolveIndexName(String releaseName) {
		return "test-release-" + releaseName.toLowerCase();
	}

	@Override
	public JobType getType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(JobContext jobContext) {
		// TODO Auto-generated method stub
		
	}
}
