using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// Class that contains the information regarding the Job Queues which are
	/// maintained by the Hadoop Map/Reduce framework.
	/// </summary>
	public class QueueInfo : Writable
	{
		private string queueName = string.Empty;

		private string schedulingInfo;

		private QueueState queueState;

		private JobStatus[] stats;

		private IList<Org.Apache.Hadoop.Mapreduce.QueueInfo> children;

		private Properties props;

		/// <summary>Default constructor for QueueInfo.</summary>
		public QueueInfo()
		{
			//The scheduling Information object is read back as String.
			//Once the scheduling information is set there is no way to recover it.
			// Jobs submitted to the queue
			// make it running by default.
			this.queueState = QueueState.Running;
			children = new AList<Org.Apache.Hadoop.Mapreduce.QueueInfo>();
			props = new Properties();
		}

		/// <summary>
		/// Construct a new QueueInfo object using the queue name and the
		/// scheduling information passed.
		/// </summary>
		/// <param name="queueName">Name of the job queue</param>
		/// <param name="schedulingInfo">
		/// Scheduling Information associated with the job
		/// queue
		/// </param>
		public QueueInfo(string queueName, string schedulingInfo)
			: this()
		{
			this.queueName = queueName;
			this.schedulingInfo = schedulingInfo;
		}

		/// <param name="queueName"/>
		/// <param name="schedulingInfo"/>
		/// <param name="state"/>
		/// <param name="stats"/>
		public QueueInfo(string queueName, string schedulingInfo, QueueState state, JobStatus
			[] stats)
			: this(queueName, schedulingInfo)
		{
			this.queueState = state;
			this.stats = stats;
		}

		/// <summary>Set the queue name of the JobQueueInfo</summary>
		/// <param name="queueName">Name of the job queue.</param>
		protected internal virtual void SetQueueName(string queueName)
		{
			this.queueName = queueName;
		}

		/// <summary>Get the queue name from JobQueueInfo</summary>
		/// <returns>queue name</returns>
		public virtual string GetQueueName()
		{
			return queueName;
		}

		/// <summary>Set the scheduling information associated to particular job queue</summary>
		/// <param name="schedulingInfo"/>
		protected internal virtual void SetSchedulingInfo(string schedulingInfo)
		{
			this.schedulingInfo = schedulingInfo;
		}

		/// <summary>Gets the scheduling information associated to particular job queue.</summary>
		/// <remarks>
		/// Gets the scheduling information associated to particular job queue.
		/// If nothing is set would return <b>"N/A"</b>
		/// </remarks>
		/// <returns>Scheduling information associated to particular Job Queue</returns>
		public virtual string GetSchedulingInfo()
		{
			if (schedulingInfo != null)
			{
				return schedulingInfo;
			}
			else
			{
				return "N/A";
			}
		}

		/// <summary>Set the state of the queue</summary>
		/// <param name="state">state of the queue.</param>
		protected internal virtual void SetState(QueueState state)
		{
			queueState = state;
		}

		/// <summary>Return the queue state</summary>
		/// <returns>the queue state.</returns>
		public virtual QueueState GetState()
		{
			return queueState;
		}

		protected internal virtual void SetJobStatuses(JobStatus[] stats)
		{
			this.stats = stats;
		}

		/// <summary>Get immediate children.</summary>
		/// <returns>list of QueueInfo</returns>
		public virtual IList<Org.Apache.Hadoop.Mapreduce.QueueInfo> GetQueueChildren()
		{
			return children;
		}

		protected internal virtual void SetQueueChildren(IList<Org.Apache.Hadoop.Mapreduce.QueueInfo
			> children)
		{
			this.children = children;
		}

		/// <summary>Get properties.</summary>
		/// <returns>Properties</returns>
		public virtual Properties GetProperties()
		{
			return props;
		}

		protected internal virtual void SetProperties(Properties props)
		{
			this.props = props;
		}

		/// <summary>Get the jobs submitted to queue</summary>
		/// <returns>list of JobStatus for the submitted jobs</returns>
		public virtual JobStatus[] GetJobStatuses()
		{
			return stats;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			queueName = StringInterner.WeakIntern(Text.ReadString(@in));
			queueState = WritableUtils.ReadEnum<QueueState>(@in);
			schedulingInfo = StringInterner.WeakIntern(Text.ReadString(@in));
			int length = @in.ReadInt();
			stats = new JobStatus[length];
			for (int i = 0; i < length; i++)
			{
				stats[i] = new JobStatus();
				stats[i].ReadFields(@in);
			}
			int count = @in.ReadInt();
			children.Clear();
			for (int i_1 = 0; i_1 < count; i_1++)
			{
				Org.Apache.Hadoop.Mapreduce.QueueInfo childQueueInfo = new Org.Apache.Hadoop.Mapreduce.QueueInfo
					();
				childQueueInfo.ReadFields(@in);
				children.AddItem(childQueueInfo);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			Text.WriteString(@out, queueName);
			WritableUtils.WriteEnum(@out, queueState);
			if (schedulingInfo != null)
			{
				Text.WriteString(@out, schedulingInfo);
			}
			else
			{
				Text.WriteString(@out, "N/A");
			}
			@out.WriteInt(stats.Length);
			foreach (JobStatus stat in stats)
			{
				stat.Write(@out);
			}
			@out.WriteInt(children.Count);
			foreach (Org.Apache.Hadoop.Mapreduce.QueueInfo childQueueInfo in children)
			{
				childQueueInfo.Write(@out);
			}
		}
	}
}
