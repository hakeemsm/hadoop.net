using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels
{
	public class RMNodeLabelsManager : CommonNodeLabelsManager
	{
		protected internal class Queue
		{
			protected internal ICollection<string> acccessibleNodeLabels;

			protected internal Resource resource;

			protected internal Queue()
			{
				acccessibleNodeLabels = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap<string
					, bool>());
				resource = Resource.NewInstance(0, 0);
			}
		}

		internal ConcurrentMap<string, RMNodeLabelsManager.Queue> queueCollections = new 
			ConcurrentHashMap<string, RMNodeLabelsManager.Queue>();

		private YarnAuthorizationProvider authorizer;

		private RMContext rmContext = null;

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			authorizer = YarnAuthorizationProvider.GetInstance(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AddLabelsToNode(IDictionary<NodeId, ICollection<string>> addedLabelsToNode
			)
		{
			try
			{
				writeLock.Lock();
				// get nodesCollection before edition
				IDictionary<string, CommonNodeLabelsManager.Host> before = CloneNodeMap(addedLabelsToNode
					.Keys);
				base.AddLabelsToNode(addedLabelsToNode);
				// get nodesCollection after edition
				IDictionary<string, CommonNodeLabelsManager.Host> after = CloneNodeMap(addedLabelsToNode
					.Keys);
				// update running nodes resources
				UpdateResourceMappings(before, after);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CheckRemoveFromClusterNodeLabelsOfQueue(ICollection
			<string> labelsToRemove)
		{
			// Check if label to remove doesn't existed or null/empty, will throw
			// exception if any of labels to remove doesn't meet requirement
			foreach (string label in labelsToRemove)
			{
				label = NormalizeLabel(label);
				// check if any queue contains this label
				foreach (KeyValuePair<string, RMNodeLabelsManager.Queue> entry in queueCollections)
				{
					string queueName = entry.Key;
					ICollection<string> queueLabels = entry.Value.acccessibleNodeLabels;
					if (queueLabels.Contains(label))
					{
						throw new IOException("Cannot remove label=" + label + ", because queue=" + queueName
							 + " is using this label. " + "Please remove label on queue before remove the label"
							);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveFromClusterNodeLabels(ICollection<string> labelsToRemove
			)
		{
			try
			{
				writeLock.Lock();
				CheckRemoveFromClusterNodeLabelsOfQueue(labelsToRemove);
				// copy before NMs
				IDictionary<string, CommonNodeLabelsManager.Host> before = CloneNodeMap();
				base.RemoveFromClusterNodeLabels(labelsToRemove);
				UpdateResourceMappings(before, nodeCollections);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveLabelsFromNode(IDictionary<NodeId, ICollection<string>
			> removeLabelsFromNode)
		{
			try
			{
				writeLock.Lock();
				// get nodesCollection before edition
				IDictionary<string, CommonNodeLabelsManager.Host> before = CloneNodeMap(removeLabelsFromNode
					.Keys);
				base.RemoveLabelsFromNode(removeLabelsFromNode);
				// get nodesCollection before edition
				IDictionary<string, CommonNodeLabelsManager.Host> after = CloneNodeMap(removeLabelsFromNode
					.Keys);
				// update running nodes resources
				UpdateResourceMappings(before, after);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReplaceLabelsOnNode(IDictionary<NodeId, ICollection<string>>
			 replaceLabelsToNode)
		{
			try
			{
				writeLock.Lock();
				// get nodesCollection before edition
				IDictionary<string, CommonNodeLabelsManager.Host> before = CloneNodeMap(replaceLabelsToNode
					.Keys);
				base.ReplaceLabelsOnNode(replaceLabelsToNode);
				// get nodesCollection after edition
				IDictionary<string, CommonNodeLabelsManager.Host> after = CloneNodeMap(replaceLabelsToNode
					.Keys);
				// update running nodes resources
				UpdateResourceMappings(before, after);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/*
		* Following methods are used for setting if a node is up and running, and it
		* will update running nodes resource
		*/
		public virtual void ActivateNode(NodeId nodeId, Resource resource)
		{
			try
			{
				writeLock.Lock();
				// save if we have a node before
				IDictionary<string, CommonNodeLabelsManager.Host> before = CloneNodeMap(ImmutableSet
					.Of(nodeId));
				CreateHostIfNonExisted(nodeId.GetHost());
				try
				{
					CreateNodeIfNonExisted(nodeId);
				}
				catch (IOException)
				{
					Log.Error("This shouldn't happen, cannot get host in nodeCollection" + " associated to the node being activated"
						);
					return;
				}
				CommonNodeLabelsManager.Node nm = GetNMInNodeSet(nodeId);
				nm.resource = resource;
				nm.running = true;
				// Add node in labelsCollection
				ICollection<string> labelsForNode = GetLabelsByNode(nodeId);
				if (labelsForNode != null)
				{
					foreach (string label in labelsForNode)
					{
						NodeLabel labelInfo = labelCollections[label];
						if (labelInfo != null)
						{
							labelInfo.AddNodeId(nodeId);
						}
					}
				}
				// get the node after edition
				IDictionary<string, CommonNodeLabelsManager.Host> after = CloneNodeMap(ImmutableSet
					.Of(nodeId));
				UpdateResourceMappings(before, after);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/*
		* Following methods are used for setting if a node unregistered to RM
		*/
		public virtual void DeactivateNode(NodeId nodeId)
		{
			try
			{
				writeLock.Lock();
				// save if we have a node before
				IDictionary<string, CommonNodeLabelsManager.Host> before = CloneNodeMap(ImmutableSet
					.Of(nodeId));
				CommonNodeLabelsManager.Node nm = GetNMInNodeSet(nodeId);
				if (null != nm)
				{
					if (null == nm.labels)
					{
						// When node deactivated, remove the nm from node collection if no
						// labels explicitly set for this particular nm
						// Save labels first, we need to remove label->nodes relation later
						ICollection<string> savedNodeLabels = GetLabelsOnNode(nodeId);
						// Remove this node in nodes collection
						Sharpen.Collections.Remove(nodeCollections[nodeId.GetHost()].nms, nodeId);
						// Remove this node in labels->node
						RemoveNodeFromLabels(nodeId, savedNodeLabels);
					}
					else
					{
						// set nm is not running, and its resource = 0
						nm.running = false;
						nm.resource = Resource.NewInstance(0, 0);
					}
				}
				// get the node after edition
				IDictionary<string, CommonNodeLabelsManager.Host> after = CloneNodeMap(ImmutableSet
					.Of(nodeId));
				UpdateResourceMappings(before, after);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void UpdateNodeResource(NodeId node, Resource newResource)
		{
			DeactivateNode(node);
			ActivateNode(node, newResource);
		}

		public virtual void ReinitializeQueueLabels(IDictionary<string, ICollection<string
			>> queueToLabels)
		{
			try
			{
				writeLock.Lock();
				// clear before set
				this.queueCollections.Clear();
				foreach (KeyValuePair<string, ICollection<string>> entry in queueToLabels)
				{
					string queue = entry.Key;
					RMNodeLabelsManager.Queue q = new RMNodeLabelsManager.Queue();
					this.queueCollections[queue] = q;
					ICollection<string> labels = entry.Value;
					if (labels.Contains(Any))
					{
						continue;
					}
					Sharpen.Collections.AddAll(q.acccessibleNodeLabels, labels);
					foreach (CommonNodeLabelsManager.Host host in nodeCollections.Values)
					{
						foreach (KeyValuePair<NodeId, CommonNodeLabelsManager.Node> nentry in host.nms)
						{
							NodeId nodeId = nentry.Key;
							CommonNodeLabelsManager.Node nm = nentry.Value;
							if (nm.running && IsNodeUsableByQueue(GetLabelsByNode(nodeId), q))
							{
								Resources.AddTo(q.resource, nm.resource);
							}
						}
					}
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetQueueResource(string
			 queueName, ICollection<string> queueLabels, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			try
			{
				readLock.Lock();
				if (queueLabels.Contains(Any))
				{
					return clusterResource;
				}
				RMNodeLabelsManager.Queue q = queueCollections[queueName];
				if (null == q)
				{
					return Resources.None();
				}
				return q.resource;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual ICollection<string> GetLabelsOnNode(NodeId nodeId)
		{
			try
			{
				readLock.Lock();
				ICollection<string> nodeLabels = GetLabelsByNode(nodeId);
				return Sharpen.Collections.UnmodifiableSet(nodeLabels);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual bool ContainsNodeLabel(string label)
		{
			try
			{
				readLock.Lock();
				return label != null && (label.IsEmpty() || labelCollections.Contains(label));
			}
			finally
			{
				readLock.Unlock();
			}
		}

		private IDictionary<string, CommonNodeLabelsManager.Host> CloneNodeMap(ICollection
			<NodeId> nodesToCopy)
		{
			IDictionary<string, CommonNodeLabelsManager.Host> map = new Dictionary<string, CommonNodeLabelsManager.Host
				>();
			foreach (NodeId nodeId in nodesToCopy)
			{
				if (!map.Contains(nodeId.GetHost()))
				{
					CommonNodeLabelsManager.Host originalN = nodeCollections[nodeId.GetHost()];
					if (null == originalN)
					{
						continue;
					}
					CommonNodeLabelsManager.Host n = originalN.Copy();
					n.nms.Clear();
					map[nodeId.GetHost()] = n;
				}
				CommonNodeLabelsManager.Host n_1 = map[nodeId.GetHost()];
				if (WildcardPort == nodeId.GetPort())
				{
					foreach (KeyValuePair<NodeId, CommonNodeLabelsManager.Node> entry in nodeCollections
						[nodeId.GetHost()].nms)
					{
						n_1.nms[entry.Key] = entry.Value.Copy();
					}
				}
				else
				{
					CommonNodeLabelsManager.Node nm = GetNMInNodeSet(nodeId);
					if (null != nm)
					{
						n_1.nms[nodeId] = nm.Copy();
					}
				}
			}
			return map;
		}

		private void UpdateResourceMappings(IDictionary<string, CommonNodeLabelsManager.Host
			> before, IDictionary<string, CommonNodeLabelsManager.Host> after)
		{
			// Get NMs in before only
			ICollection<NodeId> allNMs = new HashSet<NodeId>();
			foreach (KeyValuePair<string, CommonNodeLabelsManager.Host> entry in before)
			{
				Sharpen.Collections.AddAll(allNMs, entry.Value.nms.Keys);
			}
			foreach (KeyValuePair<string, CommonNodeLabelsManager.Host> entry_1 in after)
			{
				Sharpen.Collections.AddAll(allNMs, entry_1.Value.nms.Keys);
			}
			// Map used to notify RM
			IDictionary<NodeId, ICollection<string>> newNodeToLabelsMap = new Dictionary<NodeId
				, ICollection<string>>();
			// traverse all nms
			foreach (NodeId nodeId in allNMs)
			{
				CommonNodeLabelsManager.Node oldNM;
				if ((oldNM = GetNMInNodeSet(nodeId, before, true)) != null)
				{
					ICollection<string> oldLabels = GetLabelsByNode(nodeId, before);
					// no label in the past
					if (oldLabels.IsEmpty())
					{
						// update labels
						NodeLabel label = labelCollections[NoLabel];
						label.RemoveNode(oldNM.resource);
						// update queues, all queue can access this node
						foreach (RMNodeLabelsManager.Queue q in queueCollections.Values)
						{
							Resources.SubtractFrom(q.resource, oldNM.resource);
						}
					}
					else
					{
						// update labels
						foreach (string labelName in oldLabels)
						{
							NodeLabel label = labelCollections[labelName];
							if (null == label)
							{
								continue;
							}
							label.RemoveNode(oldNM.resource);
						}
						// update queues, only queue can access this node will be subtract
						foreach (RMNodeLabelsManager.Queue q in queueCollections.Values)
						{
							if (IsNodeUsableByQueue(oldLabels, q))
							{
								Resources.SubtractFrom(q.resource, oldNM.resource);
							}
						}
					}
				}
				CommonNodeLabelsManager.Node newNM;
				if ((newNM = GetNMInNodeSet(nodeId, after, true)) != null)
				{
					ICollection<string> newLabels = GetLabelsByNode(nodeId, after);
					newNodeToLabelsMap[nodeId] = ImmutableSet.CopyOf(newLabels);
					// no label in the past
					if (newLabels.IsEmpty())
					{
						// update labels
						NodeLabel label = labelCollections[NoLabel];
						label.AddNode(newNM.resource);
						// update queues, all queue can access this node
						foreach (RMNodeLabelsManager.Queue q in queueCollections.Values)
						{
							Resources.AddTo(q.resource, newNM.resource);
						}
					}
					else
					{
						// update labels
						foreach (string labelName in newLabels)
						{
							NodeLabel label = labelCollections[labelName];
							label.AddNode(newNM.resource);
						}
						// update queues, only queue can access this node will be subtract
						foreach (RMNodeLabelsManager.Queue q in queueCollections.Values)
						{
							if (IsNodeUsableByQueue(newLabels, q))
							{
								Resources.AddTo(q.resource, newNM.resource);
							}
						}
					}
				}
			}
			// Notify RM
			if (rmContext != null && rmContext.GetDispatcher() != null)
			{
				rmContext.GetDispatcher().GetEventHandler().Handle(new NodeLabelsUpdateSchedulerEvent
					(newNodeToLabelsMap));
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourceByLabel(string
			 label, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource)
		{
			label = NormalizeLabel(label);
			try
			{
				readLock.Lock();
				if (null == labelCollections[label])
				{
					return Resources.None();
				}
				return labelCollections[label].GetResource();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		private bool IsNodeUsableByQueue(ICollection<string> nodeLabels, RMNodeLabelsManager.Queue
			 q)
		{
			// node without any labels can be accessed by any queue
			if (nodeLabels == null || nodeLabels.IsEmpty() || (nodeLabels.Count == 1 && nodeLabels
				.Contains(NoLabel)))
			{
				return true;
			}
			foreach (string label in nodeLabels)
			{
				if (q.acccessibleNodeLabels.Contains(label))
				{
					return true;
				}
			}
			return false;
		}

		private IDictionary<string, CommonNodeLabelsManager.Host> CloneNodeMap()
		{
			ICollection<NodeId> nodesToCopy = new HashSet<NodeId>();
			foreach (string nodeName in nodeCollections.Keys)
			{
				nodesToCopy.AddItem(NodeId.NewInstance(nodeName, WildcardPort));
			}
			return CloneNodeMap(nodesToCopy);
		}

		public virtual bool CheckAccess(UserGroupInformation user)
		{
			// make sure only admin can invoke
			// this method
			if (authorizer.IsAdmin(user))
			{
				return true;
			}
			return false;
		}

		public virtual void SetRMContext(RMContext rmContext)
		{
			this.rmContext = rmContext;
		}

		public virtual IList<NodeLabel> PullRMNodeLabelsInfo()
		{
			try
			{
				readLock.Lock();
				IList<NodeLabel> infos = new AList<NodeLabel>();
				foreach (KeyValuePair<string, NodeLabel> entry in labelCollections)
				{
					NodeLabel label = entry.Value;
					infos.AddItem(label.GetCopy());
				}
				infos.Sort();
				return infos;
			}
			finally
			{
				readLock.Unlock();
			}
		}
	}
}
