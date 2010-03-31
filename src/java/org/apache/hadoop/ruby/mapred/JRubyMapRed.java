package org.apache.hadoop.ruby.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.ruby.JRubyEvaluator;
import org.apache.hadoop.mapred.JobConf;

public class JRubyMapRed {

	private JRubyEvaluator evaluator;
    protected JobConf jobconf;

	public JRubyMapRed() {
	}

	public JRubyEvaluator getJRubyEvaluator() {
		return this.evaluator;
	}

	public void configure(JobConf job) {
		evaluator = new JRubyEvaluator(job);
        jobconf = job;
	}

	public void close() throws IOException {
		// Do nothing
	}
}
