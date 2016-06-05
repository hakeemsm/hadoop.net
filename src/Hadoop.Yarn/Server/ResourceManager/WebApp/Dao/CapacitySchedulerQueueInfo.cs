using System;
using System.Collections.Generic;
using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class CapacitySchedulerQueueInfo
	{
		[XmlTransient]
		internal const float Epsilon = 1e-8f;

		[XmlTransient]
		protected internal string queuePath;

		protected internal float capacity;

		protected internal float usedCapacity;

		protected internal float maxCapacity;

		protected internal float absoluteCapacity;

		protected internal float absoluteMaxCapacity;

		protected internal float absoluteUsedCapacity;

		protected internal int numApplications;

		protected internal string queueName;

		protected internal QueueState state;

		protected internal CapacitySchedulerQueueInfoList queues;

		protected internal ResourceInfo resourcesUsed;

		private bool hideReservationQueues = false;

		protected internal AList<string> nodeLabels = new AList<string>();

		internal CapacitySchedulerQueueInfo()
		{
		}

		internal CapacitySchedulerQueueInfo(CSQueue q)
		{
			queuePath = q.GetQueuePath();
			capacity = q.GetCapacity() * 100;
			usedCapacity = q.GetUsedCapacity() * 100;
			maxCapacity = q.GetMaximumCapacity();
			if (maxCapacity < Epsilon || maxCapacity > 1f)
			{
				maxCapacity = 1f;
			}
			maxCapacity *= 100;
			absoluteCapacity = Cap(q.GetAbsoluteCapacity(), 0f, 1f) * 100;
			absoluteMaxCapacity = Cap(q.GetAbsoluteMaximumCapacity(), 0f, 1f) * 100;
			absoluteUsedCapacity = Cap(q.GetAbsoluteUsedCapacity(), 0f, 1f) * 100;
			numApplications = q.GetNumApplications();
			queueName = q.GetQueueName();
			state = q.GetState();
			resourcesUsed = new ResourceInfo(q.GetUsedResources());
			if (q is PlanQueue && !((PlanQueue)q).ShowReservationsAsQueues())
			{
				hideReservationQueues = true;
			}
			// add labels
			ICollection<string> labelSet = q.GetAccessibleNodeLabels();
			if (labelSet != null)
			{
				Sharpen.Collections.AddAll(nodeLabels, labelSet);
				nodeLabels.Sort();
			}
		}

		public virtual float GetCapacity()
		{
			return this.capacity;
		}

		public virtual float GetUsedCapacity()
		{
			return this.usedCapacity;
		}

		public virtual float GetMaxCapacity()
		{
			return this.maxCapacity;
		}

		public virtual float GetAbsoluteCapacity()
		{
			return absoluteCapacity;
		}

		public virtual float GetAbsoluteMaxCapacity()
		{
			return absoluteMaxCapacity;
		}

		public virtual float GetAbsoluteUsedCapacity()
		{
			return absoluteUsedCapacity;
		}

		public virtual int GetNumApplications()
		{
			return numApplications;
		}

		public virtual string GetQueueName()
		{
			return this.queueName;
		}

		public virtual string GetQueueState()
		{
			return this.state.ToString();
		}

		public virtual string GetQueuePath()
		{
			return this.queuePath;
		}

		public virtual CapacitySchedulerQueueInfoList GetQueues()
		{
			if (hideReservationQueues)
			{
				return new CapacitySchedulerQueueInfoList();
			}
			return this.queues;
		}

		public virtual ResourceInfo GetResourcesUsed()
		{
			return resourcesUsed;
		}

		/// <summary>Limit a value to a specified range.</summary>
		/// <param name="val">the value to be capped</param>
		/// <param name="low">the lower bound of the range (inclusive)</param>
		/// <param name="hi">the upper bound of the range (inclusive)</param>
		/// <returns>the capped value</returns>
		internal static float Cap(float val, float low, float hi)
		{
			return Math.Min(Math.Max(val, low), hi);
		}

		public virtual AList<string> GetNodeLabels()
		{
			return this.nodeLabels;
		}
	}
}
