using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Represents a YARN Cluster Node from the viewpoint of the scheduler.</summary>
	public abstract class SchedulerNode
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.SchedulerNode
			));

		private Resource availableResource = Resource.NewInstance(0, 0);

		private Resource usedResource = Resource.NewInstance(0, 0);

		private Resource totalResourceCapability;

		private RMContainer reservedContainer;

		private volatile int numContainers;

		private readonly IDictionary<ContainerId, RMContainer> launchedContainers = new Dictionary
			<ContainerId, RMContainer>();

		private readonly RMNode rmNode;

		private readonly string nodeName;

		private volatile ICollection<string> labels = null;

		public SchedulerNode(RMNode node, bool usePortForNodeName, ICollection<string> labels
			)
		{
			/* set of containers that are allocated containers */
			this.rmNode = node;
			this.availableResource = Resources.Clone(node.GetTotalCapability());
			this.totalResourceCapability = Resources.Clone(node.GetTotalCapability());
			if (usePortForNodeName)
			{
				nodeName = rmNode.GetHostName() + ":" + node.GetNodeID().GetPort();
			}
			else
			{
				nodeName = rmNode.GetHostName();
			}
			this.labels = ImmutableSet.CopyOf(labels);
		}

		public SchedulerNode(RMNode node, bool usePortForNodeName)
			: this(node, usePortForNodeName, CommonNodeLabelsManager.EmptyStringSet)
		{
		}

		public virtual RMNode GetRMNode()
		{
			return this.rmNode;
		}

		/// <summary>Set total resources on the node.</summary>
		/// <param name="resource">total resources on the node.</param>
		public virtual void SetTotalResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
			resource)
		{
			lock (this)
			{
				this.totalResourceCapability = resource;
				this.availableResource = Resources.Subtract(totalResourceCapability, this.usedResource
					);
			}
		}

		/// <summary>Get the ID of the node which contains both its hostname and port.</summary>
		/// <returns>the ID of the node</returns>
		public virtual NodeId GetNodeID()
		{
			return this.rmNode.GetNodeID();
		}

		public virtual string GetHttpAddress()
		{
			return this.rmNode.GetHttpAddress();
		}

		/// <summary>Get the name of the node for scheduling matching decisions.</summary>
		/// <remarks>
		/// Get the name of the node for scheduling matching decisions.
		/// <p>
		/// Typically this is the 'hostname' reported by the node, but it could be
		/// configured to be 'hostname:port' reported by the node via the
		/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmSchedulerIncludePortInNodeName
		/// 	"/>
		/// constant.
		/// The main usecase of this is Yarn minicluster to be able to differentiate
		/// node manager instances by their port number.
		/// </remarks>
		/// <returns>name of the node for scheduling matching decisions.</returns>
		public virtual string GetNodeName()
		{
			return nodeName;
		}

		/// <summary>Get rackname.</summary>
		/// <returns>rackname</returns>
		public virtual string GetRackName()
		{
			return this.rmNode.GetRackName();
		}

		/// <summary>
		/// The Scheduler has allocated containers on this node to the given
		/// application.
		/// </summary>
		/// <param name="rmContainer">allocated container</param>
		public virtual void AllocateContainer(RMContainer rmContainer)
		{
			lock (this)
			{
				Container container = rmContainer.GetContainer();
				DeductAvailableResource(container.GetResource());
				++numContainers;
				launchedContainers[container.GetId()] = rmContainer;
				Log.Info("Assigned container " + container.GetId() + " of capacity " + container.
					GetResource() + " on host " + rmNode.GetNodeAddress() + ", which has " + numContainers
					 + " containers, " + GetUsedResource() + " used and " + GetAvailableResource() +
					 " available after allocation");
			}
		}

		/// <summary>Get available resources on the node.</summary>
		/// <returns>available resources on the node</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetAvailableResource()
		{
			lock (this)
			{
				return this.availableResource;
			}
		}

		/// <summary>Get used resources on the node.</summary>
		/// <returns>used resources on the node</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUsedResource()
		{
			lock (this)
			{
				return this.usedResource;
			}
		}

		/// <summary>Get total resources on the node.</summary>
		/// <returns>total resources on the node.</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetTotalResource()
		{
			lock (this)
			{
				return this.totalResourceCapability;
			}
		}

		public virtual bool IsValidContainer(ContainerId containerId)
		{
			lock (this)
			{
				if (launchedContainers.Contains(containerId))
				{
					return true;
				}
				return false;
			}
		}

		private void UpdateResource(Container container)
		{
			lock (this)
			{
				AddAvailableResource(container.GetResource());
				--numContainers;
			}
		}

		/// <summary>Release an allocated container on this node.</summary>
		/// <param name="container">container to be released</param>
		public virtual void ReleaseContainer(Container container)
		{
			lock (this)
			{
				if (!IsValidContainer(container.GetId()))
				{
					Log.Error("Invalid container released " + container);
					return;
				}
				/* remove the containers from the nodemanger */
				if (null != Sharpen.Collections.Remove(launchedContainers, container.GetId()))
				{
					UpdateResource(container);
				}
				Log.Info("Released container " + container.GetId() + " of capacity " + container.
					GetResource() + " on host " + rmNode.GetNodeAddress() + ", which currently has "
					 + numContainers + " containers, " + GetUsedResource() + " used and " + GetAvailableResource
					() + " available" + ", release resources=" + true);
			}
		}

		private void AddAvailableResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource resource
			)
		{
			lock (this)
			{
				if (resource == null)
				{
					Log.Error("Invalid resource addition of null resource for " + rmNode.GetNodeAddress
						());
					return;
				}
				Resources.AddTo(availableResource, resource);
				Resources.SubtractFrom(usedResource, resource);
			}
		}

		private void DeductAvailableResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
			resource)
		{
			lock (this)
			{
				if (resource == null)
				{
					Log.Error("Invalid deduction of null resource for " + rmNode.GetNodeAddress());
					return;
				}
				Resources.SubtractFrom(availableResource, resource);
				Resources.AddTo(usedResource, resource);
			}
		}

		/// <summary>Reserve container for the attempt on this node.</summary>
		public abstract void ReserveResource(SchedulerApplicationAttempt attempt, Priority
			 priority, RMContainer container);

		/// <summary>Unreserve resources on this node.</summary>
		public abstract void UnreserveResource(SchedulerApplicationAttempt attempt);

		public override string ToString()
		{
			return "host: " + rmNode.GetNodeAddress() + " #containers=" + GetNumContainers() 
				+ " available=" + GetAvailableResource() + " used=" + GetUsedResource();
		}

		/// <summary>Get number of active containers on the node.</summary>
		/// <returns>number of active containers on the node</returns>
		public virtual int GetNumContainers()
		{
			return numContainers;
		}

		public virtual IList<RMContainer> GetRunningContainers()
		{
			lock (this)
			{
				return new AList<RMContainer>(launchedContainers.Values);
			}
		}

		public virtual RMContainer GetReservedContainer()
		{
			lock (this)
			{
				return reservedContainer;
			}
		}

		protected internal virtual void SetReservedContainer(RMContainer reservedContainer
			)
		{
			lock (this)
			{
				this.reservedContainer = reservedContainer;
			}
		}

		public virtual void RecoverContainer(RMContainer rmContainer)
		{
			lock (this)
			{
				if (rmContainer.GetState().Equals(RMContainerState.Completed))
				{
					return;
				}
				AllocateContainer(rmContainer);
			}
		}

		public virtual ICollection<string> GetLabels()
		{
			return labels;
		}

		public virtual void UpdateLabels(ICollection<string> labels)
		{
			this.labels = labels;
		}
	}
}
