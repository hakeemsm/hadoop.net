using Sharpen;

namespace org.apache.hadoop.conf
{
	public class ReconfigurationTaskStatus
	{
		internal long startTime;

		internal long endTime;

		internal readonly System.Collections.Generic.IDictionary<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
			, com.google.common.@base.Optional<string>> status;

		public ReconfigurationTaskStatus(long startTime, long endTime, System.Collections.Generic.IDictionary
			<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange, com.google.common.@base.Optional
			<string>> status)
		{
			this.startTime = startTime;
			this.endTime = endTime;
			this.status = status;
		}

		/// <summary>
		/// Return true if
		/// - A reconfiguration task has finished or
		/// - an active reconfiguration task is running
		/// </summary>
		public virtual bool hasTask()
		{
			return startTime > 0;
		}

		/// <summary>
		/// Return true if the latest reconfiguration task has finished and there is
		/// no another active task running.
		/// </summary>
		public virtual bool stopped()
		{
			return endTime > 0;
		}

		public virtual long getStartTime()
		{
			return startTime;
		}

		public virtual long getEndTime()
		{
			return endTime;
		}

		public System.Collections.Generic.IDictionary<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
			, com.google.common.@base.Optional<string>> getStatus()
		{
			return status;
		}
	}
}
