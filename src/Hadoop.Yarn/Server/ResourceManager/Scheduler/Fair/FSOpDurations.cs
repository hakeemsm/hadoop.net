using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>Class to capture the performance metrics of FairScheduler.</summary>
	/// <remarks>
	/// Class to capture the performance metrics of FairScheduler.
	/// This should be a singleton.
	/// </remarks>
	public class FSOpDurations : MetricsSource
	{
		internal MutableRate continuousSchedulingRun;

		internal MutableRate nodeUpdateCall;

		internal MutableRate updateThreadRun;

		internal MutableRate updateCall;

		internal MutableRate preemptCall;

		private static readonly MetricsInfo RecordInfo = Interns.Info("FSOpDurations", "Durations of FairScheduler calls or thread-runs"
			);

		private readonly MetricsRegistry registry;

		private bool isExtended = false;

		private static readonly Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSOpDurations
			 Instance = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSOpDurations
			();

		public static Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSOpDurations
			 GetInstance(bool isExtended)
		{
			Instance.SetExtended(isExtended);
			return Instance;
		}

		private FSOpDurations()
		{
			registry = new MetricsRegistry(RecordInfo);
			registry.Tag(RecordInfo, "FSOpDurations");
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			if (ms != null)
			{
				ms.Register(RecordInfo.Name(), RecordInfo.Description(), this);
			}
		}

		private void SetExtended(bool isExtended)
		{
			lock (this)
			{
				if (isExtended == Instance.isExtended)
				{
					return;
				}
				continuousSchedulingRun.SetExtended(isExtended);
				nodeUpdateCall.SetExtended(isExtended);
				updateThreadRun.SetExtended(isExtended);
				updateCall.SetExtended(isExtended);
				preemptCall.SetExtended(isExtended);
				Instance.isExtended = isExtended;
			}
		}

		public virtual void GetMetrics(MetricsCollector collector, bool all)
		{
			lock (this)
			{
				registry.Snapshot(collector.AddRecord(registry.Info()), all);
			}
		}

		public virtual void AddContinuousSchedulingRunDuration(long value)
		{
			continuousSchedulingRun.Add(value);
		}

		public virtual void AddNodeUpdateDuration(long value)
		{
			nodeUpdateCall.Add(value);
		}

		public virtual void AddUpdateThreadRunDuration(long value)
		{
			updateThreadRun.Add(value);
		}

		public virtual void AddUpdateCallDuration(long value)
		{
			updateCall.Add(value);
		}

		public virtual void AddPreemptCallDuration(long value)
		{
			preemptCall.Add(value);
		}
	}
}
