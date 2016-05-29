using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>
	/// Maintains a list of queues as well as scheduling parameters for each queue,
	/// such as guaranteed share allocations, from the fair scheduler config file.
	/// </summary>
	public class QueueManager
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.QueueManager
			).FullName);

		public const string RootQueue = "root";

		private readonly FairScheduler scheduler;

		private readonly ICollection<FSLeafQueue> leafQueues = new CopyOnWriteArrayList<FSLeafQueue
			>();

		private readonly IDictionary<string, FSQueue> queues = new Dictionary<string, FSQueue
			>();

		private FSParentQueue rootQueue;

		public QueueManager(FairScheduler scheduler)
		{
			this.scheduler = scheduler;
		}

		public virtual FSParentQueue GetRootQueue()
		{
			return rootQueue;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		public virtual void Initialize(Configuration conf)
		{
			rootQueue = new FSParentQueue("root", scheduler, null);
			queues[rootQueue.GetName()] = rootQueue;
			// Create the default queue
			GetLeafQueue(YarnConfiguration.DefaultQueueName, true);
		}

		/// <summary>Get a leaf queue by name, creating it if the create param is true and is necessary.
		/// 	</summary>
		/// <remarks>
		/// Get a leaf queue by name, creating it if the create param is true and is necessary.
		/// If the queue is not or can not be a leaf queue, i.e. it already exists as a
		/// parent queue, or one of the parents in its name is already a leaf queue,
		/// null is returned.
		/// The root part of the name is optional, so a queue underneath the root
		/// named "queue1" could be referred to  as just "queue1", and a queue named
		/// "queue2" underneath a parent named "parent1" that is underneath the root
		/// could be referred to as just "parent1.queue2".
		/// </remarks>
		public virtual FSLeafQueue GetLeafQueue(string name, bool create)
		{
			FSQueue queue = GetQueue(name, create, FSQueueType.Leaf);
			if (queue is FSParentQueue)
			{
				return null;
			}
			return (FSLeafQueue)queue;
		}

		/// <summary>Remove a leaf queue if empty</summary>
		/// <param name="name">name of the queue</param>
		/// <returns>true if queue was removed or false otherwise</returns>
		public virtual bool RemoveLeafQueue(string name)
		{
			name = EnsureRootPrefix(name);
			return RemoveEmptyIncompatibleQueues(name, FSQueueType.Parent);
		}

		/// <summary>Get a parent queue by name, creating it if the create param is true and is necessary.
		/// 	</summary>
		/// <remarks>
		/// Get a parent queue by name, creating it if the create param is true and is necessary.
		/// If the queue is not or can not be a parent queue, i.e. it already exists as a
		/// leaf queue, or one of the parents in its name is already a leaf queue,
		/// null is returned.
		/// The root part of the name is optional, so a queue underneath the root
		/// named "queue1" could be referred to  as just "queue1", and a queue named
		/// "queue2" underneath a parent named "parent1" that is underneath the root
		/// could be referred to as just "parent1.queue2".
		/// </remarks>
		public virtual FSParentQueue GetParentQueue(string name, bool create)
		{
			FSQueue queue = GetQueue(name, create, FSQueueType.Parent);
			if (queue is FSLeafQueue)
			{
				return null;
			}
			return (FSParentQueue)queue;
		}

		private FSQueue GetQueue(string name, bool create, FSQueueType queueType)
		{
			name = EnsureRootPrefix(name);
			lock (queues)
			{
				FSQueue queue = queues[name];
				if (queue == null && create)
				{
					// if the queue doesn't exist,create it and return
					queue = CreateQueue(name, queueType);
					// Update steady fair share for all queues
					if (queue != null)
					{
						rootQueue.RecomputeSteadyShares();
					}
				}
				return queue;
			}
		}

		/// <summary>
		/// Creates a leaf or parent queue based on what is specified in 'queueType'
		/// and places it in the tree.
		/// </summary>
		/// <remarks>
		/// Creates a leaf or parent queue based on what is specified in 'queueType'
		/// and places it in the tree. Creates any parents that don't already exist.
		/// </remarks>
		/// <returns>
		/// the created queue, if successful. null if not allowed (one of the parent
		/// queues in the queue name is already a leaf queue)
		/// </returns>
		private FSQueue CreateQueue(string name, FSQueueType queueType)
		{
			IList<string> newQueueNames = new AList<string>();
			newQueueNames.AddItem(name);
			int sepIndex = name.Length;
			FSParentQueue parent = null;
			// Move up the queue tree until we reach one that exists.
			while (sepIndex != -1)
			{
				sepIndex = name.LastIndexOf('.', sepIndex - 1);
				FSQueue queue;
				string curName = null;
				curName = Sharpen.Runtime.Substring(name, 0, sepIndex);
				queue = queues[curName];
				if (queue == null)
				{
					newQueueNames.AddItem(curName);
				}
				else
				{
					if (queue is FSParentQueue)
					{
						parent = (FSParentQueue)queue;
						break;
					}
					else
					{
						return null;
					}
				}
			}
			// At this point, parent refers to the deepest existing parent of the
			// queue to create.
			// Now that we know everything worked out, make all the queues
			// and add them to the map.
			AllocationConfiguration queueConf = scheduler.GetAllocationConfiguration();
			FSLeafQueue leafQueue = null;
			for (int i = newQueueNames.Count - 1; i >= 0; i--)
			{
				string queueName = newQueueNames[i];
				if (i == 0 && queueType != FSQueueType.Parent)
				{
					leafQueue = new FSLeafQueue(name, scheduler, parent);
					try
					{
						leafQueue.SetPolicy(queueConf.GetDefaultSchedulingPolicy());
					}
					catch (AllocationConfigurationException ex)
					{
						Log.Warn("Failed to set default scheduling policy " + queueConf.GetDefaultSchedulingPolicy
							() + " on new leaf queue.", ex);
					}
					parent.AddChildQueue(leafQueue);
					queues[leafQueue.GetName()] = leafQueue;
					leafQueues.AddItem(leafQueue);
					leafQueue.UpdatePreemptionVariables();
					return leafQueue;
				}
				else
				{
					FSParentQueue newParent = new FSParentQueue(queueName, scheduler, parent);
					try
					{
						newParent.SetPolicy(queueConf.GetDefaultSchedulingPolicy());
					}
					catch (AllocationConfigurationException ex)
					{
						Log.Warn("Failed to set default scheduling policy " + queueConf.GetDefaultSchedulingPolicy
							() + " on new parent queue.", ex);
					}
					parent.AddChildQueue(newParent);
					queues[newParent.GetName()] = newParent;
					newParent.UpdatePreemptionVariables();
					parent = newParent;
				}
			}
			return parent;
		}

		/// <summary>
		/// Make way for the given queue if possible, by removing incompatible
		/// queues with no apps in them.
		/// </summary>
		/// <remarks>
		/// Make way for the given queue if possible, by removing incompatible
		/// queues with no apps in them. Incompatibility could be due to
		/// (1) queueToCreate being currently a parent but needs to change to leaf
		/// (2) queueToCreate being currently a leaf but needs to change to parent
		/// (3) an existing leaf queue in the ancestry of queueToCreate.
		/// We will never remove the root queue or the default queue in this way.
		/// </remarks>
		/// <returns>true if we can create queueToCreate or it already exists.</returns>
		private bool RemoveEmptyIncompatibleQueues(string queueToCreate, FSQueueType queueType
			)
		{
			queueToCreate = EnsureRootPrefix(queueToCreate);
			// Ensure queueToCreate is not root and doesn't have the default queue in its
			// ancestry.
			if (queueToCreate.Equals(RootQueue) || queueToCreate.StartsWith(RootQueue + "." +
				 YarnConfiguration.DefaultQueueName + "."))
			{
				return false;
			}
			FSQueue queue = queues[queueToCreate];
			// Queue exists already.
			if (queue != null)
			{
				if (queue is FSLeafQueue)
				{
					if (queueType == FSQueueType.Leaf)
					{
						// if queue is already a leaf then return true
						return true;
					}
					// remove incompatibility since queue is a leaf currently
					// needs to change to a parent.
					return RemoveQueueIfEmpty(queue);
				}
				else
				{
					if (queueType == FSQueueType.Parent)
					{
						return true;
					}
					// If it's an existing parent queue and needs to change to leaf, 
					// remove it if it's empty.
					return RemoveQueueIfEmpty(queue);
				}
			}
			// Queue doesn't exist already. Check if the new queue would be created
			// under an existing leaf queue. If so, try removing that leaf queue.
			int sepIndex = queueToCreate.Length;
			sepIndex = queueToCreate.LastIndexOf('.', sepIndex - 1);
			while (sepIndex != -1)
			{
				string prefixString = Sharpen.Runtime.Substring(queueToCreate, 0, sepIndex);
				FSQueue prefixQueue = queues[prefixString];
				if (prefixQueue != null && prefixQueue is FSLeafQueue)
				{
					return RemoveQueueIfEmpty(prefixQueue);
				}
				sepIndex = queueToCreate.LastIndexOf('.', sepIndex - 1);
			}
			return true;
		}

		/// <summary>Remove the queue if it and its descendents are all empty.</summary>
		/// <param name="queue"/>
		/// <returns>true if removed, false otherwise</returns>
		private bool RemoveQueueIfEmpty(FSQueue queue)
		{
			if (IsEmpty(queue))
			{
				RemoveQueue(queue);
				return true;
			}
			return false;
		}

		/// <summary>Remove a queue and all its descendents.</summary>
		private void RemoveQueue(FSQueue queue)
		{
			if (queue is FSLeafQueue)
			{
				leafQueues.Remove(queue);
			}
			else
			{
				IList<FSQueue> childQueues = queue.GetChildQueues();
				while (!childQueues.IsEmpty())
				{
					RemoveQueue(childQueues[0]);
				}
			}
			Sharpen.Collections.Remove(queues, queue.GetName());
			queue.GetParent().GetChildQueues().Remove(queue);
		}

		/// <summary>
		/// Returns true if there are no applications, running or not, in the given
		/// queue or any of its descendents.
		/// </summary>
		protected internal virtual bool IsEmpty(FSQueue queue)
		{
			if (queue is FSLeafQueue)
			{
				FSLeafQueue leafQueue = (FSLeafQueue)queue;
				return queue.GetNumRunnableApps() == 0 && leafQueue.GetNumNonRunnableApps() == 0;
			}
			else
			{
				foreach (FSQueue child in queue.GetChildQueues())
				{
					if (!IsEmpty(child))
					{
						return false;
					}
				}
				return true;
			}
		}

		/// <summary>Gets a queue by name.</summary>
		public virtual FSQueue GetQueue(string name)
		{
			name = EnsureRootPrefix(name);
			lock (queues)
			{
				return queues[name];
			}
		}

		/// <summary>Return whether a queue exists already.</summary>
		public virtual bool Exists(string name)
		{
			name = EnsureRootPrefix(name);
			lock (queues)
			{
				return queues.Contains(name);
			}
		}

		/// <summary>Get a collection of all leaf queues</summary>
		public virtual ICollection<FSLeafQueue> GetLeafQueues()
		{
			lock (queues)
			{
				return leafQueues;
			}
		}

		/// <summary>Get a collection of all queues</summary>
		public virtual ICollection<FSQueue> GetQueues()
		{
			return queues.Values;
		}

		private string EnsureRootPrefix(string name)
		{
			if (!name.StartsWith(RootQueue + ".") && !name.Equals(RootQueue))
			{
				name = RootQueue + "." + name;
			}
			return name;
		}

		public virtual void UpdateAllocationConfiguration(AllocationConfiguration queueConf
			)
		{
			// Create leaf queues and the parent queues in a leaf's ancestry if they do not exist
			foreach (string name in queueConf.GetConfiguredQueues()[FSQueueType.Leaf])
			{
				if (RemoveEmptyIncompatibleQueues(name, FSQueueType.Leaf))
				{
					GetLeafQueue(name, true);
				}
			}
			// At this point all leaves and 'parents with at least one child' would have been created.
			// Now create parents with no configured leaf.
			foreach (string name_1 in queueConf.GetConfiguredQueues()[FSQueueType.Parent])
			{
				if (RemoveEmptyIncompatibleQueues(name_1, FSQueueType.Parent))
				{
					GetParentQueue(name_1, true);
				}
			}
			foreach (FSQueue queue in queues.Values)
			{
				// Update queue metrics
				FSQueueMetrics queueMetrics = queue.GetMetrics();
				queueMetrics.SetMinShare(queue.GetMinShare());
				queueMetrics.SetMaxShare(queue.GetMaxShare());
				// Set scheduling policies
				try
				{
					SchedulingPolicy policy = queueConf.GetSchedulingPolicy(queue.GetName());
					policy.Initialize(scheduler.GetClusterResource());
					queue.SetPolicy(policy);
				}
				catch (AllocationConfigurationException ex)
				{
					Log.Warn("Cannot apply configured scheduling policy to queue " + queue.GetName(), 
						ex);
				}
			}
			// Update steady fair shares for all queues
			rootQueue.RecomputeSteadyShares();
			// Update the fair share preemption timeouts and preemption for all queues
			// recursively
			rootQueue.UpdatePreemptionVariables();
		}
	}
}
