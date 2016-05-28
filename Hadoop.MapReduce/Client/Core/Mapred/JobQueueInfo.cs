using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Class that contains the information regarding the Job Queues which are
	/// maintained by the Hadoop Map/Reduce framework.
	/// </summary>
	public class JobQueueInfo : QueueInfo
	{
		/// <summary>Default constructor for Job Queue Info.</summary>
		public JobQueueInfo()
			: base()
		{
		}

		/// <summary>
		/// Construct a new JobQueueInfo object using the queue name and the
		/// scheduling information passed.
		/// </summary>
		/// <param name="queueName">Name of the job queue</param>
		/// <param name="schedulingInfo">
		/// Scheduling Information associated with the job
		/// queue
		/// </param>
		public JobQueueInfo(string queueName, string schedulingInfo)
			: base(queueName, schedulingInfo)
		{
		}

		internal JobQueueInfo(QueueInfo queue)
			: this(queue.GetQueueName(), queue.GetSchedulingInfo())
		{
			SetQueueState(queue.GetState().GetStateName());
			SetQueueChildren(queue.GetQueueChildren());
			SetProperties(queue.GetProperties());
			SetJobStatuses(queue.GetJobStatuses());
		}

		/// <summary>Set the queue name of the JobQueueInfo</summary>
		/// <param name="queueName">Name of the job queue.</param>
		[InterfaceAudience.Private]
		protected internal override void SetQueueName(string queueName)
		{
			base.SetQueueName(queueName);
		}

		/// <summary>Set the scheduling information associated to particular job queue</summary>
		/// <param name="schedulingInfo"/>
		[InterfaceAudience.Private]
		protected internal override void SetSchedulingInfo(string schedulingInfo)
		{
			base.SetSchedulingInfo(schedulingInfo);
		}

		/// <summary>Set the state of the queue</summary>
		/// <param name="state">state of the queue.</param>
		[InterfaceAudience.Private]
		public virtual void SetQueueState(string state)
		{
			base.SetState(QueueState.GetState(state));
		}

		/// <summary>Use getState() instead</summary>
		[Obsolete]
		public virtual string GetQueueState()
		{
			return base.GetState().ToString();
		}

		[InterfaceAudience.Private]
		public virtual void SetChildren(IList<Org.Apache.Hadoop.Mapred.JobQueueInfo> children
			)
		{
			IList<QueueInfo> list = new AList<QueueInfo>();
			foreach (Org.Apache.Hadoop.Mapred.JobQueueInfo q in children)
			{
				list.AddItem(q);
			}
			base.SetQueueChildren(list);
		}

		public virtual IList<Org.Apache.Hadoop.Mapred.JobQueueInfo> GetChildren()
		{
			IList<Org.Apache.Hadoop.Mapred.JobQueueInfo> list = new AList<Org.Apache.Hadoop.Mapred.JobQueueInfo
				>();
			foreach (QueueInfo q in base.GetQueueChildren())
			{
				list.AddItem((Org.Apache.Hadoop.Mapred.JobQueueInfo)q);
			}
			return list;
		}

		[InterfaceAudience.Private]
		protected internal override void SetProperties(Properties props)
		{
			base.SetProperties(props);
		}

		/// <summary>
		/// Add a child
		/// <see cref="JobQueueInfo"/>
		/// to this
		/// <see cref="JobQueueInfo"/>
		/// . Modify the
		/// fully-qualified name of the child
		/// <see cref="JobQueueInfo"/>
		/// to reflect the
		/// hierarchy.
		/// Only for testing.
		/// </summary>
		/// <param name="child"/>
		internal virtual void AddChild(Org.Apache.Hadoop.Mapred.JobQueueInfo child)
		{
			IList<Org.Apache.Hadoop.Mapred.JobQueueInfo> children = GetChildren();
			children.AddItem(child);
			SetChildren(children);
		}

		/// <summary>
		/// Remove the child from this
		/// <see cref="JobQueueInfo"/>
		/// . This also resets the
		/// queue-name of the child from a fully-qualified name to a simple queue name.
		/// Only for testing.
		/// </summary>
		/// <param name="child"/>
		internal virtual void RemoveChild(Org.Apache.Hadoop.Mapred.JobQueueInfo child)
		{
			IList<Org.Apache.Hadoop.Mapred.JobQueueInfo> children = GetChildren();
			children.Remove(child);
			SetChildren(children);
		}

		[InterfaceAudience.Private]
		protected internal override void SetJobStatuses(JobStatus[] stats)
		{
			base.SetJobStatuses(stats);
		}
	}
}
