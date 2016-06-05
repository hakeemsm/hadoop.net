using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>QueueInfo is a report of the runtime information of the queue.</summary>
	/// <remarks>
	/// QueueInfo is a report of the runtime information of the queue.
	/// <p>
	/// It includes information such as:
	/// <ul>
	/// <li>Queue name.</li>
	/// <li>Capacity of the queue.</li>
	/// <li>Maximum capacity of the queue.</li>
	/// <li>Current capacity of the queue.</li>
	/// <li>Child queues.</li>
	/// <li>Running applications.</li>
	/// <li>
	/// <see cref="QueueState"/>
	/// of the queue.</li>
	/// </ul>
	/// </remarks>
	/// <seealso cref="QueueState"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetQueueInfo(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueInfoRequest)
	/// 	"/>
	public abstract class QueueInfo
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static QueueInfo NewInstance(string queueName, float capacity, float maximumCapacity
			, float currentCapacity, IList<QueueInfo> childQueues, IList<ApplicationReport> 
			applications, QueueState queueState, ICollection<string> accessibleNodeLabels, string
			 defaultNodeLabelExpression)
		{
			QueueInfo queueInfo = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<QueueInfo>();
			queueInfo.SetQueueName(queueName);
			queueInfo.SetCapacity(capacity);
			queueInfo.SetMaximumCapacity(maximumCapacity);
			queueInfo.SetCurrentCapacity(currentCapacity);
			queueInfo.SetChildQueues(childQueues);
			queueInfo.SetApplications(applications);
			queueInfo.SetQueueState(queueState);
			queueInfo.SetAccessibleNodeLabels(accessibleNodeLabels);
			queueInfo.SetDefaultNodeLabelExpression(defaultNodeLabelExpression);
			return queueInfo;
		}

		/// <summary>Get the <em>name</em> of the queue.</summary>
		/// <returns><em>name</em> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetQueueName();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetQueueName(string queueName);

		/// <summary>Get the <em>configured capacity</em> of the queue.</summary>
		/// <returns><em>configured capacity</em> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract float GetCapacity();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetCapacity(float capacity);

		/// <summary>Get the <em>maximum capacity</em> of the queue.</summary>
		/// <returns><em>maximum capacity</em> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract float GetMaximumCapacity();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetMaximumCapacity(float maximumCapacity);

		/// <summary>Get the <em>current capacity</em> of the queue.</summary>
		/// <returns><em>current capacity</em> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract float GetCurrentCapacity();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetCurrentCapacity(float currentCapacity);

		/// <summary>Get the <em>child queues</em> of the queue.</summary>
		/// <returns><em>child queues</em> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<QueueInfo> GetChildQueues();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetChildQueues(IList<QueueInfo> childQueues);

		/// <summary>Get the <em>running applications</em> of the queue.</summary>
		/// <returns><em>running applications</em> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ApplicationReport> GetApplications();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplications(IList<ApplicationReport> applications);

		/// <summary>Get the <code>QueueState</code> of the queue.</summary>
		/// <returns><code>QueueState</code> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract QueueState GetQueueState();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetQueueState(QueueState queueState);

		/// <summary>Get the <code>accessible node labels</code> of the queue.</summary>
		/// <returns><code>accessible node labels</code> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ICollection<string> GetAccessibleNodeLabels();

		/// <summary>Set the <code>accessible node labels</code> of the queue.</summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetAccessibleNodeLabels(ICollection<string> labels);

		/// <summary>
		/// Get the <code>default node label expression</code> of the queue, this takes
		/// affect only when the <code>ApplicationSubmissionContext</code> and
		/// <code>ResourceRequest</code> don't specify their
		/// <code>NodeLabelExpression</code>.
		/// </summary>
		/// <returns><code>default node label expression</code> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetDefaultNodeLabelExpression();

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetDefaultNodeLabelExpression(string defaultLabelExpression);
	}
}
