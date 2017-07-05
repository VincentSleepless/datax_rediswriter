package com.alibaba.datax.plugin.writer.rediswriter;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;


public class RedisWriter extends Writer {

	public static class Job extends Writer.Job {

		private static final Logger LOG = LoggerFactory.getLogger(Job.class);
		private RedisWriterMasterProxy proxy = new RedisWriterMasterProxy();

		@Override
		public void init() {
			LOG.info("init() begin ...");
			try {
				
				LOG.info("Peer plugin "+ this.getPeerPluginName()+" info:"+this.getPeerPluginJobConf().toJSON());
				LOG.info("Current plugin info:"+this.getPluginJobConf().toJSON());
				//LOG.info("Job conf info:"+this.);
				
				
				
				//get configuration
				this.proxy.init(this.getPluginJobConf(),
						this.getPeerPluginJobConf(),this.getPeerPluginName());
			} catch (Exception e) {
				LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
				throw DataXException.asDataXException(RedisError.UNKNOWN,
						e.getMessage(), e);
			}
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			try {
				return this.proxy.split(mandatoryNumber);
			} catch (Exception e) {
				LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
				throw DataXException.asDataXException(RedisError.SPLIT_ERROR,
						e.getMessage(), e);
			}
		}

		/*
		 * (non-Javadoc) job全局清理工作关闭连接
		 * @see com.alibaba.datax.common.plugin.Pluginable#destroy()
		 */
		@Override
		public void destroy() {
			this.proxy.close();
		}

		@Override
		public void prepare() {
			// TODO Auto-generated method stub
			super.prepare();
		}

	}

	public static class Task extends Writer.Task {

	    private static final Logger LOG = LoggerFactory.getLogger(Task.class);
	    
	    private RedisWriterSlaveProxy proxy = new RedisWriterSlaveProxy();
		
		
		@Override
		public void init() {
			// TODO Auto-generated method stub
		}

		@Override
		public void destroy() {
			// TODO Auto-generated method stub
			proxy.close();
		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {

			try {
				//this.getPeerPluginJobConf();
				
				
				this.proxy.init(this.getPluginJobConf());
				this.proxy.write(lineReceiver, this.getTaskPluginCollector());
			} catch (Exception e) {
				// TODO: handle exception
				  LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
	                throw DataXException.asDataXException(RedisError.WRITE_ERROR,e.getMessage(), e);
			}
			
		}

	}

}
