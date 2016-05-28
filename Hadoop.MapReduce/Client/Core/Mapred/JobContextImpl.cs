/* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class JobContextImpl : Org.Apache.Hadoop.Mapreduce.Task.JobContextImpl, JobContext
	{
		private JobConf job;

		private Progressable progress;

		public JobContextImpl(JobConf conf, JobID jobId, Progressable progress)
			: base(conf, jobId)
		{
			this.job = conf;
			this.progress = progress;
		}

		public JobContextImpl(JobConf conf, JobID jobId)
			: this(conf, jobId, Reporter.Null)
		{
		}

		/// <summary>Get the job Configuration</summary>
		/// <returns>JobConf</returns>
		public virtual JobConf GetJobConf()
		{
			return job;
		}

		/// <summary>Get the progress mechanism for reporting progress.</summary>
		/// <returns>progress mechanism</returns>
		public virtual Progressable GetProgressible()
		{
			return progress;
		}
	}
}
