using System.Collections.Generic;
using Com.Google.Common.Base;


namespace Org.Apache.Hadoop.Conf
{
	public class ReconfigurationTaskStatus
	{
		internal long startTime;

		internal long endTime;

		internal readonly IDictionary<ReconfigurationUtil.PropertyChange, Optional<string
			>> status;

		public ReconfigurationTaskStatus(long startTime, long endTime, IDictionary<ReconfigurationUtil.PropertyChange
			, Optional<string>> status)
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
		public virtual bool HasTask()
		{
			return startTime > 0;
		}

		/// <summary>
		/// Return true if the latest reconfiguration task has finished and there is
		/// no another active task running.
		/// </summary>
		public virtual bool Stopped()
		{
			return endTime > 0;
		}

		public virtual long GetStartTime()
		{
			return startTime;
		}

		public virtual long GetEndTime()
		{
			return endTime;
		}

		public IDictionary<ReconfigurationUtil.PropertyChange, Optional<string>> GetStatus
			()
		{
			return status;
		}
	}
}
