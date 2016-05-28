using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public class JobHistoryEvent : AbstractEvent<EventType>
	{
		private readonly JobId jobID;

		private readonly HistoryEvent historyEvent;

		public JobHistoryEvent(JobId jobID, HistoryEvent historyEvent)
			: this(jobID, historyEvent, Runtime.CurrentTimeMillis())
		{
		}

		public JobHistoryEvent(JobId jobID, HistoryEvent historyEvent, long timestamp)
			: base(historyEvent.GetEventType(), timestamp)
		{
			this.jobID = jobID;
			this.historyEvent = historyEvent;
		}

		public virtual JobId GetJobID()
		{
			return jobID;
		}

		public virtual HistoryEvent GetHistoryEvent()
		{
			return historyEvent;
		}
	}
}
