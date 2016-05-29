using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Nodelabels.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels
{
	public class CommonNodeLabelsManager : AbstractService
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Nodelabels.CommonNodeLabelsManager
			));

		private const int MaxLabelLength = 255;

		public static readonly ICollection<string> EmptyStringSet = Sharpen.Collections.UnmodifiableSet
			(new HashSet<string>(0));

		public const string Any = "*";

		public static readonly ICollection<string> AccessAnyLabelSet = ImmutableSet.Of(Any
			);

		private static readonly Sharpen.Pattern LabelPattern = Sharpen.Pattern.Compile("^[0-9a-zA-Z][0-9a-zA-Z-_]*"
			);

		public const int WildcardPort = 0;

		/// <summary>Error messages</summary>
		[VisibleForTesting]
		public const string NodeLabelsNotEnabledErr = "Node-label-based scheduling is disabled. Please check "
			 + YarnConfiguration.NodeLabelsEnabled;

		/// <summary>
		/// If a user doesn't specify label of a queue or node, it belongs
		/// DEFAULT_LABEL
		/// </summary>
		public const string NoLabel = string.Empty;

		protected internal Dispatcher dispatcher;

		protected internal ConcurrentMap<string, NodeLabel> labelCollections = new ConcurrentHashMap
			<string, NodeLabel>();

		protected internal ConcurrentMap<string, CommonNodeLabelsManager.Host> nodeCollections
			 = new ConcurrentHashMap<string, CommonNodeLabelsManager.Host>();

		protected internal readonly ReentrantReadWriteLock.ReadLock readLock;

		protected internal readonly ReentrantReadWriteLock.WriteLock writeLock;

		protected internal NodeLabelsStore store;

		private bool nodeLabelsEnabled = false;

		/// <summary>A <code>Host</code> can have multiple <code>Node</code>s</summary>
		protected internal class Host
		{
			public ICollection<string> labels;

			public IDictionary<NodeId, CommonNodeLabelsManager.Node> nms;

			protected internal Host()
			{
				labels = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap<string, bool>());
				nms = new ConcurrentHashMap<NodeId, CommonNodeLabelsManager.Node>();
			}

			public virtual CommonNodeLabelsManager.Host Copy()
			{
				CommonNodeLabelsManager.Host c = new CommonNodeLabelsManager.Host();
				c.labels = new HashSet<string>(labels);
				foreach (KeyValuePair<NodeId, CommonNodeLabelsManager.Node> entry in nms)
				{
					c.nms[entry.Key] = entry.Value.Copy();
				}
				return c;
			}
		}

		protected internal class Node
		{
			public ICollection<string> labels;

			public Resource resource;

			public bool running;

			public NodeId nodeId;

			protected internal Node(NodeId nodeid)
			{
				labels = null;
				resource = Resource.NewInstance(0, 0);
				running = false;
				nodeId = nodeid;
			}

			public virtual CommonNodeLabelsManager.Node Copy()
			{
				CommonNodeLabelsManager.Node c = new CommonNodeLabelsManager.Node(nodeId);
				if (labels != null)
				{
					c.labels = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap<string, bool>(
						));
					Sharpen.Collections.AddAll(c.labels, labels);
				}
				else
				{
					c.labels = null;
				}
				c.resource = Resources.Clone(resource);
				c.running = running;
				return c;
			}
		}

		private enum NodeLabelUpdateOperation
		{
			Add,
			Remove,
			Replace
		}

		private sealed class ForwardingEventHandler : EventHandler<NodeLabelsStoreEvent>
		{
			public void Handle(NodeLabelsStoreEvent @event)
			{
				this._enclosing.HandleStoreEvent(@event);
			}

			internal ForwardingEventHandler(CommonNodeLabelsManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CommonNodeLabelsManager _enclosing;
		}

		// Dispatcher related code
		protected internal virtual void HandleStoreEvent(NodeLabelsStoreEvent @event)
		{
			try
			{
				switch (@event.GetType())
				{
					case NodeLabelsStoreEventType.AddLabels:
					{
						StoreNewClusterNodeLabels storeNewClusterNodeLabelsEvent = (StoreNewClusterNodeLabels
							)@event;
						store.StoreNewClusterNodeLabels(storeNewClusterNodeLabelsEvent.GetLabels());
						break;
					}

					case NodeLabelsStoreEventType.RemoveLabels:
					{
						RemoveClusterNodeLabels removeClusterNodeLabelsEvent = (RemoveClusterNodeLabels)@event;
						store.RemoveClusterNodeLabels(removeClusterNodeLabelsEvent.GetLabels());
						break;
					}

					case NodeLabelsStoreEventType.StoreNodeToLabels:
					{
						UpdateNodeToLabelsMappingsEvent updateNodeToLabelsMappingsEvent = (UpdateNodeToLabelsMappingsEvent
							)@event;
						store.UpdateNodeToLabelsMappings(updateNodeToLabelsMappingsEvent.GetNodeToLabels(
							));
						break;
					}
				}
			}
			catch (IOException e)
			{
				Log.Error("Failed to store label modification to storage");
				throw new YarnRuntimeException(e);
			}
		}

		public CommonNodeLabelsManager()
			: base(typeof(CommonNodeLabelsManager).FullName)
		{
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			readLock = Lock.ReadLock();
			writeLock = Lock.WriteLock();
		}

		// for UT purpose
		protected internal virtual void InitDispatcher(Configuration conf)
		{
			// create async handler
			dispatcher = new AsyncDispatcher();
			AsyncDispatcher asyncDispatcher = (AsyncDispatcher)dispatcher;
			asyncDispatcher.Init(conf);
			asyncDispatcher.SetDrainEventsOnStop();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			// set if node labels enabled
			nodeLabelsEnabled = conf.GetBoolean(YarnConfiguration.NodeLabelsEnabled, YarnConfiguration
				.DefaultNodeLabelsEnabled);
			labelCollections[NoLabel] = new NodeLabel(NoLabel);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void InitNodeLabelStore(Configuration conf)
		{
			this.store = new FileSystemNodeLabelsStore(this);
			this.store.Init(conf);
			this.store.Recover();
		}

		// for UT purpose
		protected internal virtual void StartDispatcher()
		{
			// start dispatcher
			AsyncDispatcher asyncDispatcher = (AsyncDispatcher)dispatcher;
			asyncDispatcher.Start();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (nodeLabelsEnabled)
			{
				InitNodeLabelStore(GetConfig());
			}
			// init dispatcher only when service start, because recover will happen in
			// service init, we don't want to trigger any event handling at that time.
			InitDispatcher(GetConfig());
			if (null != dispatcher)
			{
				dispatcher.Register(typeof(NodeLabelsStoreEventType), new CommonNodeLabelsManager.ForwardingEventHandler
					(this));
			}
			StartDispatcher();
		}

		// for UT purpose
		protected internal virtual void StopDispatcher()
		{
			AsyncDispatcher asyncDispatcher = (AsyncDispatcher)dispatcher;
			asyncDispatcher.Stop();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			// finalize store
			StopDispatcher();
			// only close store when we enabled store persistent
			if (null != store)
			{
				store.Close();
			}
		}

		/// <summary>Add multiple node labels to repository</summary>
		/// <param name="labels">new node labels added</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddToCluserNodeLabels(ICollection<string> labels)
		{
			if (!nodeLabelsEnabled)
			{
				Log.Error(NodeLabelsNotEnabledErr);
				throw new IOException(NodeLabelsNotEnabledErr);
			}
			if (null == labels || labels.IsEmpty())
			{
				return;
			}
			ICollection<string> newLabels = new HashSet<string>();
			labels = NormalizeLabels(labels);
			// do a check before actual adding them, will throw exception if any of them
			// doesn't meet label name requirement
			foreach (string label in labels)
			{
				CheckAndThrowLabelName(label);
			}
			foreach (string label_1 in labels)
			{
				// shouldn't overwrite it to avoid changing the Label.resource
				if (this.labelCollections[label_1] == null)
				{
					this.labelCollections[label_1] = new NodeLabel(label_1);
					newLabels.AddItem(label_1);
				}
			}
			if (null != dispatcher && !newLabels.IsEmpty())
			{
				dispatcher.GetEventHandler().Handle(new StoreNewClusterNodeLabels(newLabels));
			}
			Log.Info("Add labels: [" + StringUtils.Join(labels.GetEnumerator(), ",") + "]");
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CheckAddLabelsToNode(IDictionary<NodeId, ICollection
			<string>> addedLabelsToNode)
		{
			if (null == addedLabelsToNode || addedLabelsToNode.IsEmpty())
			{
				return;
			}
			// check all labels being added existed
			ICollection<string> knownLabels = labelCollections.Keys;
			foreach (KeyValuePair<NodeId, ICollection<string>> entry in addedLabelsToNode)
			{
				NodeId nodeId = entry.Key;
				ICollection<string> labels = entry.Value;
				if (!knownLabels.ContainsAll(labels))
				{
					string msg = "Not all labels being added contained by known " + "label collections, please check"
						 + ", added labels=[" + StringUtils.Join(labels, ",") + "]";
					Log.Error(msg);
					throw new IOException(msg);
				}
				// In YARN-2694, we temporarily disable user add more than 1 labels on a
				// same host
				if (!labels.IsEmpty())
				{
					ICollection<string> newLabels = new HashSet<string>(GetLabelsByNode(nodeId));
					Sharpen.Collections.AddAll(newLabels, labels);
					// we don't allow number of labels on a node > 1 after added labels
					if (newLabels.Count > 1)
					{
						string msg = string.Format("%d labels specified on host=%s after add labels to node"
							 + ", please note that we do not support specifying multiple" + " labels on a single host for now."
							, newLabels.Count, nodeId.GetHost());
						Log.Error(msg);
						throw new IOException(msg);
					}
				}
			}
		}

		/// <summary>add more labels to nodes</summary>
		/// <param name="addedLabelsToNode">
		/// node
		/// <literal>-&gt;</literal>
		/// labels map
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddLabelsToNode(IDictionary<NodeId, ICollection<string>> addedLabelsToNode
			)
		{
			if (!nodeLabelsEnabled)
			{
				Log.Error(NodeLabelsNotEnabledErr);
				throw new IOException(NodeLabelsNotEnabledErr);
			}
			addedLabelsToNode = NormalizeNodeIdToLabels(addedLabelsToNode);
			CheckAddLabelsToNode(addedLabelsToNode);
			InternalUpdateLabelsOnNodes(addedLabelsToNode, CommonNodeLabelsManager.NodeLabelUpdateOperation
				.Add);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CheckRemoveFromClusterNodeLabels(ICollection<string
			> labelsToRemove)
		{
			if (null == labelsToRemove || labelsToRemove.IsEmpty())
			{
				return;
			}
			// Check if label to remove doesn't existed or null/empty, will throw
			// exception if any of labels to remove doesn't meet requirement
			foreach (string label in labelsToRemove)
			{
				label = NormalizeLabel(label);
				if (label == null || label.IsEmpty())
				{
					throw new IOException("Label to be removed is null or empty");
				}
				if (!labelCollections.Contains(label))
				{
					throw new IOException("Node label=" + label + " to be removed doesn't existed in cluster "
						 + "node labels collection.");
				}
			}
		}

		protected internal virtual void InternalRemoveFromClusterNodeLabels(ICollection<string
			> labelsToRemove)
		{
			// remove labels from nodes
			foreach (KeyValuePair<string, CommonNodeLabelsManager.Host> nodeEntry in nodeCollections)
			{
				CommonNodeLabelsManager.Host host = nodeEntry.Value;
				if (null != host)
				{
					host.labels.RemoveAll(labelsToRemove);
					foreach (CommonNodeLabelsManager.Node nm in host.nms.Values)
					{
						if (nm.labels != null)
						{
							nm.labels.RemoveAll(labelsToRemove);
						}
					}
				}
			}
			// remove labels from node labels collection
			foreach (string label in labelsToRemove)
			{
				Sharpen.Collections.Remove(labelCollections, label);
			}
			// create event to remove labels
			if (null != dispatcher)
			{
				dispatcher.GetEventHandler().Handle(new RemoveClusterNodeLabels(labelsToRemove));
			}
			Log.Info("Remove labels: [" + StringUtils.Join(labelsToRemove.GetEnumerator(), ","
				) + "]");
		}

		/// <summary>Remove multiple node labels from repository</summary>
		/// <param name="labelsToRemove">node labels to remove</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveFromClusterNodeLabels(ICollection<string> labelsToRemove
			)
		{
			if (!nodeLabelsEnabled)
			{
				Log.Error(NodeLabelsNotEnabledErr);
				throw new IOException(NodeLabelsNotEnabledErr);
			}
			labelsToRemove = NormalizeLabels(labelsToRemove);
			CheckRemoveFromClusterNodeLabels(labelsToRemove);
			InternalRemoveFromClusterNodeLabels(labelsToRemove);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CheckRemoveLabelsFromNode(IDictionary<NodeId, ICollection
			<string>> removeLabelsFromNode)
		{
			// check all labels being added existed
			ICollection<string> knownLabels = labelCollections.Keys;
			foreach (KeyValuePair<NodeId, ICollection<string>> entry in removeLabelsFromNode)
			{
				NodeId nodeId = entry.Key;
				ICollection<string> labels = entry.Value;
				if (!knownLabels.ContainsAll(labels))
				{
					string msg = "Not all labels being removed contained by known " + "label collections, please check"
						 + ", removed labels=[" + StringUtils.Join(labels, ",") + "]";
					Log.Error(msg);
					throw new IOException(msg);
				}
				ICollection<string> originalLabels = null;
				bool nodeExisted = false;
				if (WildcardPort != nodeId.GetPort())
				{
					CommonNodeLabelsManager.Node nm = GetNMInNodeSet(nodeId);
					if (nm != null)
					{
						originalLabels = nm.labels;
						nodeExisted = true;
					}
				}
				else
				{
					CommonNodeLabelsManager.Host host = nodeCollections[nodeId.GetHost()];
					if (null != host)
					{
						originalLabels = host.labels;
						nodeExisted = true;
					}
				}
				if (!nodeExisted)
				{
					string msg = "Try to remove labels from NM=" + nodeId + ", but the NM doesn't existed";
					Log.Error(msg);
					throw new IOException(msg);
				}
				// the labels will never be null
				if (labels.IsEmpty())
				{
					continue;
				}
				// originalLabels may be null,
				// because when a Node is created, Node.labels can be null.
				if (originalLabels == null || !originalLabels.ContainsAll(labels))
				{
					string msg = "Try to remove labels = [" + StringUtils.Join(labels, ",") + "], but not all labels contained by NM="
						 + nodeId;
					Log.Error(msg);
					throw new IOException(msg);
				}
			}
		}

		private void AddNodeToLabels(NodeId node, ICollection<string> labels)
		{
			foreach (string l in labels)
			{
				labelCollections[l].AddNodeId(node);
			}
		}

		protected internal virtual void RemoveNodeFromLabels(NodeId node, ICollection<string
			> labels)
		{
			foreach (string l in labels)
			{
				labelCollections[l].RemoveNodeId(node);
			}
		}

		private void ReplaceNodeForLabels(NodeId node, ICollection<string> oldLabels, ICollection
			<string> newLabels)
		{
			if (oldLabels != null)
			{
				RemoveNodeFromLabels(node, oldLabels);
			}
			AddNodeToLabels(node, newLabels);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void InternalUpdateLabelsOnNodes(IDictionary<NodeId, ICollection
			<string>> nodeToLabels, CommonNodeLabelsManager.NodeLabelUpdateOperation op)
		{
			// do update labels from nodes
			IDictionary<NodeId, ICollection<string>> newNMToLabels = new Dictionary<NodeId, ICollection
				<string>>();
			ICollection<string> oldLabels;
			foreach (KeyValuePair<NodeId, ICollection<string>> entry in nodeToLabels)
			{
				NodeId nodeId = entry.Key;
				ICollection<string> labels = entry.Value;
				CreateHostIfNonExisted(nodeId.GetHost());
				if (nodeId.GetPort() == WildcardPort)
				{
					CommonNodeLabelsManager.Host host = nodeCollections[nodeId.GetHost()];
					switch (op)
					{
						case CommonNodeLabelsManager.NodeLabelUpdateOperation.Remove:
						{
							RemoveNodeFromLabels(nodeId, labels);
							host.labels.RemoveAll(labels);
							foreach (CommonNodeLabelsManager.Node node in host.nms.Values)
							{
								if (node.labels != null)
								{
									node.labels.RemoveAll(labels);
								}
								RemoveNodeFromLabels(node.nodeId, labels);
							}
							break;
						}

						case CommonNodeLabelsManager.NodeLabelUpdateOperation.Add:
						{
							AddNodeToLabels(nodeId, labels);
							Sharpen.Collections.AddAll(host.labels, labels);
							foreach (CommonNodeLabelsManager.Node node_1 in host.nms.Values)
							{
								if (node_1.labels != null)
								{
									Sharpen.Collections.AddAll(node_1.labels, labels);
								}
								AddNodeToLabels(node_1.nodeId, labels);
							}
							break;
						}

						case CommonNodeLabelsManager.NodeLabelUpdateOperation.Replace:
						{
							ReplaceNodeForLabels(nodeId, host.labels, labels);
							host.labels.Clear();
							Sharpen.Collections.AddAll(host.labels, labels);
							foreach (CommonNodeLabelsManager.Node node_2 in host.nms.Values)
							{
								ReplaceNodeForLabels(node_2.nodeId, node_2.labels, labels);
								node_2.labels = null;
							}
							break;
						}

						default:
						{
							break;
						}
					}
					newNMToLabels[nodeId] = host.labels;
				}
				else
				{
					if (EnumSet.Of(CommonNodeLabelsManager.NodeLabelUpdateOperation.Add, CommonNodeLabelsManager.NodeLabelUpdateOperation
						.Replace).Contains(op))
					{
						// Add and replace
						CreateNodeIfNonExisted(nodeId);
						CommonNodeLabelsManager.Node nm = GetNMInNodeSet(nodeId);
						switch (op)
						{
							case CommonNodeLabelsManager.NodeLabelUpdateOperation.Add:
							{
								AddNodeToLabels(nodeId, labels);
								if (nm.labels == null)
								{
									nm.labels = new HashSet<string>();
								}
								Sharpen.Collections.AddAll(nm.labels, labels);
								break;
							}

							case CommonNodeLabelsManager.NodeLabelUpdateOperation.Replace:
							{
								oldLabels = GetLabelsByNode(nodeId);
								ReplaceNodeForLabels(nodeId, oldLabels, labels);
								if (nm.labels == null)
								{
									nm.labels = new HashSet<string>();
								}
								nm.labels.Clear();
								Sharpen.Collections.AddAll(nm.labels, labels);
								break;
							}

							default:
							{
								break;
							}
						}
						newNMToLabels[nodeId] = nm.labels;
					}
					else
					{
						// remove
						RemoveNodeFromLabels(nodeId, labels);
						CommonNodeLabelsManager.Node nm = GetNMInNodeSet(nodeId);
						if (nm.labels != null)
						{
							nm.labels.RemoveAll(labels);
							newNMToLabels[nodeId] = nm.labels;
						}
					}
				}
			}
			if (null != dispatcher)
			{
				dispatcher.GetEventHandler().Handle(new UpdateNodeToLabelsMappingsEvent(newNMToLabels
					));
			}
			// shows node->labels we added
			Log.Info(op.ToString() + " labels on nodes:");
			foreach (KeyValuePair<NodeId, ICollection<string>> entry_1 in newNMToLabels)
			{
				Log.Info("  NM=" + entry_1.Key + ", labels=[" + StringUtils.Join(entry_1.Value.GetEnumerator
					(), ",") + "]");
			}
		}

		/// <summary>
		/// remove labels from nodes, labels being removed most be contained by these
		/// nodes
		/// </summary>
		/// <param name="removeLabelsFromNode">
		/// node
		/// <literal>-&gt;</literal>
		/// labels map
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveLabelsFromNode(IDictionary<NodeId, ICollection<string>>
			 removeLabelsFromNode)
		{
			if (!nodeLabelsEnabled)
			{
				Log.Error(NodeLabelsNotEnabledErr);
				throw new IOException(NodeLabelsNotEnabledErr);
			}
			removeLabelsFromNode = NormalizeNodeIdToLabels(removeLabelsFromNode);
			CheckRemoveLabelsFromNode(removeLabelsFromNode);
			InternalUpdateLabelsOnNodes(removeLabelsFromNode, CommonNodeLabelsManager.NodeLabelUpdateOperation
				.Remove);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CheckReplaceLabelsOnNode(IDictionary<NodeId, ICollection
			<string>> replaceLabelsToNode)
		{
			if (null == replaceLabelsToNode || replaceLabelsToNode.IsEmpty())
			{
				return;
			}
			// check all labels being added existed
			ICollection<string> knownLabels = labelCollections.Keys;
			foreach (KeyValuePair<NodeId, ICollection<string>> entry in replaceLabelsToNode)
			{
				NodeId nodeId = entry.Key;
				ICollection<string> labels = entry.Value;
				// As in YARN-2694, we disable user add more than 1 labels on a same host
				if (labels.Count > 1)
				{
					string msg = string.Format("%d labels specified on host=%s" + ", please note that we do not support specifying multiple"
						 + " labels on a single host for now.", labels.Count, nodeId.GetHost());
					Log.Error(msg);
					throw new IOException(msg);
				}
				if (!knownLabels.ContainsAll(labels))
				{
					string msg = "Not all labels being replaced contained by known " + "label collections, please check"
						 + ", new labels=[" + StringUtils.Join(labels, ",") + "]";
					Log.Error(msg);
					throw new IOException(msg);
				}
			}
		}

		/// <summary>replace labels to nodes</summary>
		/// <param name="replaceLabelsToNode">
		/// node
		/// <literal>-&gt;</literal>
		/// labels map
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReplaceLabelsOnNode(IDictionary<NodeId, ICollection<string>> 
			replaceLabelsToNode)
		{
			if (!nodeLabelsEnabled)
			{
				Log.Error(NodeLabelsNotEnabledErr);
				throw new IOException(NodeLabelsNotEnabledErr);
			}
			replaceLabelsToNode = NormalizeNodeIdToLabels(replaceLabelsToNode);
			CheckReplaceLabelsOnNode(replaceLabelsToNode);
			InternalUpdateLabelsOnNodes(replaceLabelsToNode, CommonNodeLabelsManager.NodeLabelUpdateOperation
				.Replace);
		}

		/// <summary>Get mapping of nodes to labels</summary>
		/// <returns>nodes to labels map</returns>
		public virtual IDictionary<NodeId, ICollection<string>> GetNodeLabels()
		{
			try
			{
				readLock.Lock();
				IDictionary<NodeId, ICollection<string>> nodeToLabels = new Dictionary<NodeId, ICollection
					<string>>();
				foreach (KeyValuePair<string, CommonNodeLabelsManager.Host> entry in nodeCollections)
				{
					string hostName = entry.Key;
					CommonNodeLabelsManager.Host host = entry.Value;
					foreach (NodeId nodeId in host.nms.Keys)
					{
						ICollection<string> nodeLabels = GetLabelsByNode(nodeId);
						if (nodeLabels == null || nodeLabels.IsEmpty())
						{
							continue;
						}
						nodeToLabels[nodeId] = nodeLabels;
					}
					if (!host.labels.IsEmpty())
					{
						nodeToLabels[NodeId.NewInstance(hostName, WildcardPort)] = host.labels;
					}
				}
				return Sharpen.Collections.UnmodifiableMap(nodeToLabels);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>Get mapping of labels to nodes for all the labels.</summary>
		/// <returns>labels to nodes map</returns>
		public virtual IDictionary<string, ICollection<NodeId>> GetLabelsToNodes()
		{
			try
			{
				readLock.Lock();
				return GetLabelsToNodes(labelCollections.Keys);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>Get mapping of labels to nodes for specified set of labels.</summary>
		/// <param name="labels">
		/// set of labels for which labels to nodes mapping will be
		/// returned.
		/// </param>
		/// <returns>labels to nodes map</returns>
		public virtual IDictionary<string, ICollection<NodeId>> GetLabelsToNodes(ICollection
			<string> labels)
		{
			try
			{
				readLock.Lock();
				IDictionary<string, ICollection<NodeId>> labelsToNodes = new Dictionary<string, ICollection
					<NodeId>>();
				foreach (string label in labels)
				{
					if (label.Equals(NoLabel))
					{
						continue;
					}
					NodeLabel nodeLabelInfo = labelCollections[label];
					if (nodeLabelInfo != null)
					{
						ICollection<NodeId> nodeIds = nodeLabelInfo.GetAssociatedNodeIds();
						if (!nodeIds.IsEmpty())
						{
							labelsToNodes[label] = nodeIds;
						}
					}
					else
					{
						Log.Warn("getLabelsToNodes : Label [" + label + "] cannot be found");
					}
				}
				return Sharpen.Collections.UnmodifiableMap(labelsToNodes);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>Get existing valid labels in repository</summary>
		/// <returns>existing valid labels in repository</returns>
		public virtual ICollection<string> GetClusterNodeLabels()
		{
			try
			{
				readLock.Lock();
				ICollection<string> labels = new HashSet<string>(labelCollections.Keys);
				labels.Remove(NoLabel);
				return Sharpen.Collections.UnmodifiableSet(labels);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckAndThrowLabelName(string label)
		{
			if (label == null || label.IsEmpty() || label.Length > MaxLabelLength)
			{
				throw new IOException("label added is empty or exceeds " + MaxLabelLength + " character(s)"
					);
			}
			label = label.Trim();
			bool match = LabelPattern.Matcher(label).Matches();
			if (!match)
			{
				throw new IOException("label name should only contains " + "{0-9, a-z, A-Z, -, _} and should not started with {-,_}"
					 + ", now it is=" + label);
			}
		}

		protected internal virtual string NormalizeLabel(string label)
		{
			if (label != null)
			{
				return label.Trim();
			}
			return NoLabel;
		}

		private ICollection<string> NormalizeLabels(ICollection<string> labels)
		{
			ICollection<string> newLabels = new HashSet<string>();
			foreach (string label in labels)
			{
				newLabels.AddItem(NormalizeLabel(label));
			}
			return newLabels;
		}

		protected internal virtual CommonNodeLabelsManager.Node GetNMInNodeSet(NodeId nodeId
			)
		{
			return GetNMInNodeSet(nodeId, nodeCollections);
		}

		protected internal virtual CommonNodeLabelsManager.Node GetNMInNodeSet(NodeId nodeId
			, IDictionary<string, CommonNodeLabelsManager.Host> map)
		{
			return GetNMInNodeSet(nodeId, map, false);
		}

		protected internal virtual CommonNodeLabelsManager.Node GetNMInNodeSet(NodeId nodeId
			, IDictionary<string, CommonNodeLabelsManager.Host> map, bool checkRunning)
		{
			CommonNodeLabelsManager.Host host = map[nodeId.GetHost()];
			if (null == host)
			{
				return null;
			}
			CommonNodeLabelsManager.Node nm = host.nms[nodeId];
			if (null == nm)
			{
				return null;
			}
			if (checkRunning)
			{
				return nm.running ? nm : null;
			}
			return nm;
		}

		protected internal virtual ICollection<string> GetLabelsByNode(NodeId nodeId)
		{
			return GetLabelsByNode(nodeId, nodeCollections);
		}

		protected internal virtual ICollection<string> GetLabelsByNode(NodeId nodeId, IDictionary
			<string, CommonNodeLabelsManager.Host> map)
		{
			CommonNodeLabelsManager.Host host = map[nodeId.GetHost()];
			if (null == host)
			{
				return EmptyStringSet;
			}
			CommonNodeLabelsManager.Node nm = host.nms[nodeId];
			if (null != nm && null != nm.labels)
			{
				return nm.labels;
			}
			else
			{
				return host.labels;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CreateNodeIfNonExisted(NodeId nodeId)
		{
			CommonNodeLabelsManager.Host host = nodeCollections[nodeId.GetHost()];
			if (null == host)
			{
				throw new IOException("Should create host before creating node.");
			}
			CommonNodeLabelsManager.Node nm = host.nms[nodeId];
			if (null == nm)
			{
				host.nms[nodeId] = new CommonNodeLabelsManager.Node(nodeId);
			}
		}

		protected internal virtual void CreateHostIfNonExisted(string hostName)
		{
			CommonNodeLabelsManager.Host host = nodeCollections[hostName];
			if (null == host)
			{
				host = new CommonNodeLabelsManager.Host();
				nodeCollections[hostName] = host;
			}
		}

		protected internal virtual IDictionary<NodeId, ICollection<string>> NormalizeNodeIdToLabels
			(IDictionary<NodeId, ICollection<string>> nodeIdToLabels)
		{
			IDictionary<NodeId, ICollection<string>> newMap = new Dictionary<NodeId, ICollection
				<string>>();
			foreach (KeyValuePair<NodeId, ICollection<string>> entry in nodeIdToLabels)
			{
				NodeId id = entry.Key;
				ICollection<string> labels = entry.Value;
				newMap[id] = NormalizeLabels(labels);
			}
			return newMap;
		}
	}
}
