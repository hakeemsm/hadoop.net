using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A class for storing the properties of a job queue.</summary>
	internal class Queue : Comparable<Org.Apache.Hadoop.Mapred.Queue>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Queue
			));

		private string name = null;

		private IDictionary<string, AccessControlList> acls;

		private QueueState state = QueueState.Running;

		private object schedulingInfo;

		private ICollection<Org.Apache.Hadoop.Mapred.Queue> children;

		private Properties props;

		/// <summary>Default constructor is useful in creating the hierarchy.</summary>
		/// <remarks>
		/// Default constructor is useful in creating the hierarchy.
		/// The variables are populated using mutator methods.
		/// </remarks>
		internal Queue()
		{
		}

		/// <summary>Create a job queue</summary>
		/// <param name="name">name of the queue</param>
		/// <param name="acls">ACLs for the queue</param>
		/// <param name="state">state of the queue</param>
		internal Queue(string name, IDictionary<string, AccessControlList> acls, QueueState
			 state)
		{
			//Queue name
			//acls list
			//Queue State
			// An Object that can be used by schedulers to fill in
			// arbitrary scheduling information. The toString method
			// of these objects will be called by the framework to
			// get a String that can be displayed on UI.
			this.name = name;
			this.acls = acls;
			this.state = state;
		}

		/// <summary>Return the name of the queue</summary>
		/// <returns>name of the queue</returns>
		internal virtual string GetName()
		{
			return name;
		}

		/// <summary>Set the name of the queue</summary>
		/// <param name="name">name of the queue</param>
		internal virtual void SetName(string name)
		{
			this.name = name;
		}

		/// <summary>
		/// Return the ACLs for the queue
		/// The keys in the map indicate the operations that can be performed,
		/// and the values indicate the list of users/groups who can perform
		/// the operation.
		/// </summary>
		/// <returns>
		/// Map containing the operations that can be performed and
		/// who can perform the operations.
		/// </returns>
		internal virtual IDictionary<string, AccessControlList> GetAcls()
		{
			return acls;
		}

		/// <summary>Set the ACLs for the queue</summary>
		/// <param name="acls">
		/// Map containing the operations that can be performed and
		/// who can perform the operations.
		/// </param>
		internal virtual void SetAcls(IDictionary<string, AccessControlList> acls)
		{
			this.acls = acls;
		}

		/// <summary>Return the state of the queue.</summary>
		/// <returns>state of the queue</returns>
		internal virtual QueueState GetState()
		{
			return state;
		}

		/// <summary>Set the state of the queue.</summary>
		/// <param name="state">state of the queue.</param>
		internal virtual void SetState(QueueState state)
		{
			this.state = state;
		}

		/// <summary>Return the scheduling information for the queue</summary>
		/// <returns>scheduling information for the queue.</returns>
		internal virtual object GetSchedulingInfo()
		{
			return schedulingInfo;
		}

		/// <summary>Set the scheduling information from the queue.</summary>
		/// <param name="schedulingInfo">scheduling information for the queue.</param>
		internal virtual void SetSchedulingInfo(object schedulingInfo)
		{
			this.schedulingInfo = schedulingInfo;
		}

		/// <summary>
		/// Copy the scheduling information from the sourceQueue into this queue
		/// recursively.
		/// </summary>
		/// <param name="sourceQueue"/>
		internal virtual void CopySchedulingInfo(Org.Apache.Hadoop.Mapred.Queue sourceQueue
			)
		{
			// First update the children queues recursively.
			ICollection<Org.Apache.Hadoop.Mapred.Queue> destChildren = GetChildren();
			if (destChildren != null)
			{
				IEnumerator<Org.Apache.Hadoop.Mapred.Queue> itr1 = destChildren.GetEnumerator();
				IEnumerator<Org.Apache.Hadoop.Mapred.Queue> itr2 = sourceQueue.GetChildren().GetEnumerator
					();
				while (itr1.HasNext())
				{
					itr1.Next().CopySchedulingInfo(itr2.Next());
				}
			}
			// Now, copy the information for the root-queue itself
			SetSchedulingInfo(sourceQueue.GetSchedulingInfo());
		}

		internal virtual void AddChild(Org.Apache.Hadoop.Mapred.Queue child)
		{
			if (children == null)
			{
				children = new TreeSet<Org.Apache.Hadoop.Mapred.Queue>();
			}
			children.AddItem(child);
		}

		/// <returns/>
		internal virtual ICollection<Org.Apache.Hadoop.Mapred.Queue> GetChildren()
		{
			return children;
		}

		/// <param name="props"/>
		internal virtual void SetProperties(Properties props)
		{
			this.props = props;
		}

		/// <returns/>
		internal virtual Properties GetProperties()
		{
			return this.props;
		}

		/// <summary>
		/// This methods helps in traversing the
		/// tree hierarchy.
		/// </summary>
		/// <remarks>
		/// This methods helps in traversing the
		/// tree hierarchy.
		/// Returns list of all inner queues.i.e nodes which has children.
		/// below this level.
		/// Incase of children being null , returns an empty map.
		/// This helps in case of creating union of inner and leaf queues.
		/// </remarks>
		/// <returns/>
		internal virtual IDictionary<string, Org.Apache.Hadoop.Mapred.Queue> GetInnerQueues
			()
		{
			IDictionary<string, Org.Apache.Hadoop.Mapred.Queue> l = new Dictionary<string, Org.Apache.Hadoop.Mapred.Queue
				>();
			//If no children , return empty set.
			//This check is required for root node.
			if (children == null)
			{
				return l;
			}
			//check for children if they are parent.
			foreach (Org.Apache.Hadoop.Mapred.Queue child in children)
			{
				//check if children are themselves parent add them
				if (child.GetChildren() != null && child.GetChildren().Count > 0)
				{
					l[child.GetName()] = child;
					l.PutAll(child.GetInnerQueues());
				}
			}
			return l;
		}

		/// <summary>
		/// This method helps in maintaining the single
		/// data structure across QueueManager.
		/// </summary>
		/// <remarks>
		/// This method helps in maintaining the single
		/// data structure across QueueManager.
		/// Now if we just maintain list of root queues we
		/// should be done.
		/// Doesn't return null .
		/// Adds itself if this is leaf node.
		/// </remarks>
		/// <returns/>
		internal virtual IDictionary<string, Org.Apache.Hadoop.Mapred.Queue> GetLeafQueues
			()
		{
			IDictionary<string, Org.Apache.Hadoop.Mapred.Queue> l = new Dictionary<string, Org.Apache.Hadoop.Mapred.Queue
				>();
			if (children == null)
			{
				l[name] = this;
				return l;
			}
			foreach (Org.Apache.Hadoop.Mapred.Queue child in children)
			{
				l.PutAll(child.GetLeafQueues());
			}
			return l;
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Mapred.Queue queue)
		{
			return string.CompareOrdinal(name, queue.GetName());
		}

		public override bool Equals(object o)
		{
			if (o == this)
			{
				return true;
			}
			if (!(o is Org.Apache.Hadoop.Mapred.Queue))
			{
				return false;
			}
			return ((Org.Apache.Hadoop.Mapred.Queue)o).GetName().Equals(name);
		}

		public override string ToString()
		{
			return this.GetName();
		}

		public override int GetHashCode()
		{
			return this.GetName().GetHashCode();
		}

		/// <summary>
		/// Return hierarchy of
		/// <see cref="JobQueueInfo"/>
		/// objects
		/// under this Queue.
		/// </summary>
		/// <returns>JobQueueInfo[]</returns>
		internal virtual JobQueueInfo GetJobQueueInfo()
		{
			JobQueueInfo queueInfo = new JobQueueInfo();
			queueInfo.SetQueueName(name);
			Log.Debug("created jobQInfo " + queueInfo.GetQueueName());
			queueInfo.SetQueueState(state.GetStateName());
			if (schedulingInfo != null)
			{
				queueInfo.SetSchedulingInfo(schedulingInfo.ToString());
			}
			if (props != null)
			{
				//Create deep copy of properties.
				Properties newProps = new Properties();
				foreach (object key in props.Keys)
				{
					newProps.SetProperty(key.ToString(), props.GetProperty(key.ToString()));
				}
				queueInfo.SetProperties(newProps);
			}
			if (children != null && children.Count > 0)
			{
				IList<JobQueueInfo> list = new AList<JobQueueInfo>();
				foreach (Org.Apache.Hadoop.Mapred.Queue child in children)
				{
					list.AddItem(child.GetJobQueueInfo());
				}
				queueInfo.SetChildren(list);
			}
			return queueInfo;
		}

		/// <summary>For each node validate if current node hierarchy is same newState.</summary>
		/// <remarks>
		/// For each node validate if current node hierarchy is same newState.
		/// recursively check for child nodes.
		/// </remarks>
		/// <param name="newState"/>
		/// <returns/>
		internal virtual bool IsHierarchySameAs(Org.Apache.Hadoop.Mapred.Queue newState)
		{
			if (newState == null)
			{
				return false;
			}
			//First check if names are equal
			if (!(name.Equals(newState.GetName())))
			{
				Log.Info(" current name " + name + " not equal to " + newState.GetName());
				return false;
			}
			if (children == null || children.Count == 0)
			{
				if (newState.GetChildren() != null && newState.GetChildren().Count > 0)
				{
					Log.Info(newState + " has added children in refresh ");
					return false;
				}
			}
			else
			{
				if (children.Count > 0)
				{
					//check for the individual children and then see if all of them
					//are updated.
					if (newState.GetChildren() == null)
					{
						Log.Fatal("In the current state, queue " + GetName() + " has " + children.Count +
							 " but the new state has none!");
						return false;
					}
					int childrenSize = children.Count;
					int newChildrenSize = newState.GetChildren().Count;
					if (childrenSize != newChildrenSize)
					{
						Log.Fatal("Number of children for queue " + newState.GetName() + " in newState is "
							 + newChildrenSize + " which is not equal to " + childrenSize + " in the current state."
							);
						return false;
					}
					//children are pre sorted as they are stored in treeset.
					//hence order shold be the same.
					IEnumerator<Org.Apache.Hadoop.Mapred.Queue> itr1 = children.GetEnumerator();
					IEnumerator<Org.Apache.Hadoop.Mapred.Queue> itr2 = newState.GetChildren().GetEnumerator
						();
					while (itr1.HasNext())
					{
						Org.Apache.Hadoop.Mapred.Queue q = itr1.Next();
						Org.Apache.Hadoop.Mapred.Queue newq = itr2.Next();
						if (!(q.IsHierarchySameAs(newq)))
						{
							Log.Info(" Queue " + q.GetName() + " not equal to " + newq.GetName());
							return false;
						}
					}
				}
			}
			return true;
		}
	}
}
