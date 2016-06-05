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
	public class TaskAttemptContextImpl : Org.Apache.Hadoop.Mapreduce.Task.TaskAttemptContextImpl
		, TaskAttemptContext
	{
		private Reporter reporter;

		public TaskAttemptContextImpl(JobConf conf, TaskAttemptID taskid)
			: this(conf, taskid, Reporter.Null)
		{
		}

		internal TaskAttemptContextImpl(JobConf conf, TaskAttemptID taskid, Reporter reporter
			)
			: base(conf, taskid)
		{
			this.reporter = reporter;
		}

		/// <summary>Get the taskAttemptID.</summary>
		/// <returns>TaskAttemptID</returns>
		public override TaskAttemptID GetTaskAttemptID()
		{
			return (TaskAttemptID)base.GetTaskAttemptID();
		}

		public virtual Progressable GetProgressible()
		{
			return reporter;
		}

		public virtual JobConf GetJobConf()
		{
			return (JobConf)GetConfiguration();
		}

		public override float GetProgress()
		{
			return reporter.GetProgress();
		}

		public override Counter GetCounter<_T0>(Enum<_T0> counterName)
		{
			return reporter.GetCounter(counterName);
		}

		public override Counter GetCounter(string groupName, string counterName)
		{
			return reporter.GetCounter(groupName, counterName);
		}

		/// <summary>Report progress.</summary>
		public override void Progress()
		{
			reporter.Progress();
		}

		/// <summary>Set the current status of the task to the given string.</summary>
		public override void SetStatus(string status)
		{
			SetStatusString(status);
			reporter.SetStatus(status);
		}
	}
}
