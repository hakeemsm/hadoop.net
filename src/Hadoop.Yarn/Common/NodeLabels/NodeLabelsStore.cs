using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels
{
	public abstract class NodeLabelsStore : IDisposable
	{
		protected internal readonly CommonNodeLabelsManager mgr;

		public NodeLabelsStore(CommonNodeLabelsManager mgr)
		{
			this.mgr = mgr;
		}

		/// <summary>
		/// Store node
		/// <literal>-&gt;</literal>
		/// label
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void UpdateNodeToLabelsMappings(IDictionary<NodeId, ICollection<string
			>> nodeToLabels);

		/// <summary>Store new labels</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreNewClusterNodeLabels(ICollection<string> label);

		/// <summary>Remove labels</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveClusterNodeLabels(ICollection<string> labels);

		/// <summary>Recover labels and node to labels mappings from store</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Recover();

		/// <exception cref="System.Exception"/>
		public virtual void Init(Configuration conf)
		{
		}

		public virtual CommonNodeLabelsManager GetNodeLabelsManager()
		{
			return mgr;
		}

		public abstract void Close();
	}
}
