using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobCounterUpdateEvent : JobEvent
	{
		internal IList<JobCounterUpdateEvent.CounterIncrementalUpdate> counterUpdates = null;

		public JobCounterUpdateEvent(JobId jobId)
			: base(jobId, JobEventType.JobCounterUpdate)
		{
			counterUpdates = new AList<JobCounterUpdateEvent.CounterIncrementalUpdate>();
		}

		public virtual void AddCounterUpdate<_T0>(Enum<_T0> key, long incrValue)
			where _T0 : Enum<E>
		{
			counterUpdates.AddItem(new JobCounterUpdateEvent.CounterIncrementalUpdate(key, incrValue
				));
		}

		public virtual IList<JobCounterUpdateEvent.CounterIncrementalUpdate> GetCounterUpdates
			()
		{
			return counterUpdates;
		}

		public class CounterIncrementalUpdate
		{
			internal Enum<object> key;

			internal long incrValue;

			public CounterIncrementalUpdate(Enum<object> key, long incrValue)
			{
				this.key = key;
				this.incrValue = incrValue;
			}

			public virtual Enum<object> GetCounterKey()
			{
				return key;
			}

			public virtual long GetIncrementValue()
			{
				return incrValue;
			}
		}
	}
}
