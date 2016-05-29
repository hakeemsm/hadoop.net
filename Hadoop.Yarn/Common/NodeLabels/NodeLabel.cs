using System.Collections.Generic;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels
{
	public class NodeLabel : Comparable<Org.Apache.Hadoop.Yarn.Nodelabels.NodeLabel>
	{
		private Resource resource;

		private int numActiveNMs;

		private string labelName;

		private ICollection<NodeId> nodeIds;

		public NodeLabel(string labelName)
			: this(labelName, Resource.NewInstance(0, 0), 0)
		{
		}

		protected internal NodeLabel(string labelName, Resource res, int activeNMs)
		{
			this.labelName = labelName;
			this.resource = res;
			this.numActiveNMs = activeNMs;
			this.nodeIds = new HashSet<NodeId>();
		}

		public virtual void AddNodeId(NodeId node)
		{
			nodeIds.AddItem(node);
		}

		public virtual void RemoveNodeId(NodeId node)
		{
			nodeIds.Remove(node);
		}

		public virtual ICollection<NodeId> GetAssociatedNodeIds()
		{
			return new HashSet<NodeId>(nodeIds);
		}

		public virtual void AddNode(Resource nodeRes)
		{
			Resources.AddTo(resource, nodeRes);
			numActiveNMs++;
		}

		public virtual void RemoveNode(Org.Apache.Hadoop.Yarn.Api.Records.Resource nodeRes
			)
		{
			Resources.SubtractFrom(resource, nodeRes);
			numActiveNMs--;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResource()
		{
			return this.resource;
		}

		public virtual int GetNumActiveNMs()
		{
			return numActiveNMs;
		}

		public virtual string GetLabelName()
		{
			return labelName;
		}

		public virtual Org.Apache.Hadoop.Yarn.Nodelabels.NodeLabel GetCopy()
		{
			return new Org.Apache.Hadoop.Yarn.Nodelabels.NodeLabel(labelName, resource, numActiveNMs
				);
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Yarn.Nodelabels.NodeLabel o)
		{
			// We should always put empty label entry first after sorting
			if (labelName.IsEmpty() != o.GetLabelName().IsEmpty())
			{
				if (labelName.IsEmpty())
				{
					return -1;
				}
				return 1;
			}
			return string.CompareOrdinal(labelName, o.GetLabelName());
		}

		public override bool Equals(object obj)
		{
			if (obj is Org.Apache.Hadoop.Yarn.Nodelabels.NodeLabel)
			{
				Org.Apache.Hadoop.Yarn.Nodelabels.NodeLabel other = (Org.Apache.Hadoop.Yarn.Nodelabels.NodeLabel
					)obj;
				return Resources.Equals(resource, other.GetResource()) && StringUtils.Equals(labelName
					, other.GetLabelName()) && (other.GetNumActiveNMs() == numActiveNMs);
			}
			return false;
		}

		public override int GetHashCode()
		{
			int prime = 502357;
			return (int)((((long)labelName.GetHashCode() << 8) + (resource.GetHashCode() << 4
				) + numActiveNMs) % prime);
		}
	}
}
