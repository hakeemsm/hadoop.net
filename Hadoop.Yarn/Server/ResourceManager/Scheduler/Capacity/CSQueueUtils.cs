using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	internal class CSQueueUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(CSQueueUtils));

		internal const float Epsilon = 0.0001f;

		/*
		* Used only by tests
		*/
		public static void CheckMaxCapacity(string queueName, float capacity, float maximumCapacity
			)
		{
			if (maximumCapacity < 0.0f || maximumCapacity > 1.0f)
			{
				throw new ArgumentException("Illegal value  of maximumCapacity " + maximumCapacity
					 + " used in call to setMaxCapacity for queue " + queueName);
			}
		}

		/*
		* Used only by tests
		*/
		public static void CheckAbsoluteCapacity(string queueName, float absCapacity, float
			 absMaxCapacity)
		{
			if (absMaxCapacity < (absCapacity - Epsilon))
			{
				throw new ArgumentException("Illegal call to setMaxCapacity. " + "Queue '" + queueName
					 + "' has " + "an absolute capacity (" + absCapacity + ") greater than " + "its absolute maximumCapacity ("
					 + absMaxCapacity + ")");
			}
		}

		/// <summary>
		/// Check sanity of capacities:
		/// - capacity &lt;= maxCapacity
		/// - absCapacity &lt;= absMaximumCapacity
		/// </summary>
		private static void CapacitiesSanityCheck(string queueName, QueueCapacities queueCapacities
			)
		{
			foreach (string label in queueCapacities.GetExistingNodeLabels())
			{
				float capacity = queueCapacities.GetCapacity(label);
				float maximumCapacity = queueCapacities.GetMaximumCapacity(label);
				if (capacity > maximumCapacity)
				{
					throw new ArgumentException("Illegal queue capacity setting, " + "(capacity=" + capacity
						 + ") > (maximum-capacity=" + maximumCapacity + "). When label=[" + label + "]");
				}
				// Actually, this may not needed since we have verified capacity <=
				// maximumCapacity. And the way we compute absolute capacity (abs(x) =
				// cap(x) * cap(x.parent) * ...) is a monotone increasing function. But
				// just keep it here to make sure our compute abs capacity method works
				// correctly. 
				float absCapacity = queueCapacities.GetAbsoluteCapacity(label);
				float absMaxCapacity = queueCapacities.GetAbsoluteMaximumCapacity(label);
				if (absCapacity > absMaxCapacity)
				{
					throw new ArgumentException("Illegal queue capacity setting, " + "(abs-capacity="
						 + absCapacity + ") > (abs-maximum-capacity=" + absMaxCapacity + "). When label=["
						 + label + "]");
				}
			}
		}

		public static float ComputeAbsoluteMaximumCapacity(float maximumCapacity, CSQueue
			 parent)
		{
			float parentAbsMaxCapacity = (parent == null) ? 1.0f : parent.GetAbsoluteMaximumCapacity
				();
			return (parentAbsMaxCapacity * maximumCapacity);
		}

		/// <summary>
		/// This method intends to be used by ReservationQueue, ReservationQueue will
		/// not appear in configuration file, so we shouldn't do load capacities
		/// settings in configuration for reservation queue.
		/// </summary>
		public static void UpdateAndCheckCapacitiesByLabel(string queuePath, QueueCapacities
			 queueCapacities, QueueCapacities parentQueueCapacities)
		{
			UpdateAbsoluteCapacitiesByNodeLabels(queueCapacities, parentQueueCapacities);
			CapacitiesSanityCheck(queuePath, queueCapacities);
		}

		/// <summary>
		/// Do following steps for capacities
		/// - Load capacities from configuration
		/// - Update absolute capacities for new capacities
		/// - Check if capacities/absolute-capacities legal
		/// </summary>
		public static void LoadUpdateAndCheckCapacities(string queuePath, CapacitySchedulerConfiguration
			 csConf, QueueCapacities queueCapacities, QueueCapacities parentQueueCapacities)
		{
			LoadCapacitiesByLabelsFromConf(queuePath, queueCapacities, csConf);
			UpdateAbsoluteCapacitiesByNodeLabels(queueCapacities, parentQueueCapacities);
			CapacitiesSanityCheck(queuePath, queueCapacities);
		}

		private static void LoadCapacitiesByLabelsFromConf(string queuePath, QueueCapacities
			 queueCapacities, CapacitySchedulerConfiguration csConf)
		{
			queueCapacities.ClearConfigurableFields();
			ICollection<string> configuredNodelabels = csConf.GetConfiguredNodeLabels(queuePath
				);
			foreach (string label in configuredNodelabels)
			{
				if (label.Equals(CommonNodeLabelsManager.NoLabel))
				{
					queueCapacities.SetCapacity(CommonNodeLabelsManager.NoLabel, csConf.GetNonLabeledQueueCapacity
						(queuePath) / 100);
					queueCapacities.SetMaximumCapacity(CommonNodeLabelsManager.NoLabel, csConf.GetNonLabeledQueueMaximumCapacity
						(queuePath) / 100);
				}
				else
				{
					queueCapacities.SetCapacity(label, csConf.GetLabeledQueueCapacity(queuePath, label
						) / 100);
					queueCapacities.SetMaximumCapacity(label, csConf.GetLabeledQueueMaximumCapacity(queuePath
						, label) / 100);
				}
			}
		}

		// Set absolute capacities for {capacity, maximum-capacity}
		private static void UpdateAbsoluteCapacitiesByNodeLabels(QueueCapacities queueCapacities
			, QueueCapacities parentQueueCapacities)
		{
			foreach (string label in queueCapacities.GetExistingNodeLabels())
			{
				float capacity = queueCapacities.GetCapacity(label);
				if (capacity > 0f)
				{
					queueCapacities.SetAbsoluteCapacity(label, capacity * (parentQueueCapacities == null
						 ? 1 : parentQueueCapacities.GetAbsoluteCapacity(label)));
				}
				float maxCapacity = queueCapacities.GetMaximumCapacity(label);
				if (maxCapacity > 0f)
				{
					queueCapacities.SetAbsoluteMaximumCapacity(label, maxCapacity * (parentQueueCapacities
						 == null ? 1 : parentQueueCapacities.GetAbsoluteMaximumCapacity(label)));
				}
			}
		}

		public static void UpdateQueueStatistics(ResourceCalculator calculator, CSQueue childQueue
			, CSQueue parentQueue, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumAllocation)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueLimit = Resources.None();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource usedResources = childQueue.GetUsedResources
				();
			float absoluteUsedCapacity = 0.0f;
			float usedCapacity = 0.0f;
			if (Resources.GreaterThan(calculator, clusterResource, clusterResource, Resources
				.None()))
			{
				queueLimit = Resources.Multiply(clusterResource, childQueue.GetAbsoluteCapacity()
					);
				absoluteUsedCapacity = Resources.Divide(calculator, clusterResource, usedResources
					, clusterResource);
				usedCapacity = Resources.Equals(queueLimit, Resources.None()) ? 0 : Resources.Divide
					(calculator, clusterResource, usedResources, queueLimit);
			}
			childQueue.SetUsedCapacity(usedCapacity);
			childQueue.SetAbsoluteUsedCapacity(absoluteUsedCapacity);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource available = Resources.Subtract(queueLimit
				, usedResources);
			childQueue.GetMetrics().SetAvailableResourcesToQueue(Resources.Max(calculator, clusterResource
				, available, Resources.None()));
		}
	}
}
