using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels
{
	public class NullRMNodeLabelsManager : RMNodeLabelsManager
	{
		internal IDictionary<NodeId, ICollection<string>> lastNodeToLabels = null;

		internal ICollection<string> lastAddedlabels = null;

		internal ICollection<string> lastRemovedlabels = null;

		protected override void InitNodeLabelStore(Configuration conf)
		{
			this.store = new _NodeLabelsStore_38(this);
		}

		private sealed class _NodeLabelsStore_38 : NodeLabelsStore
		{
			public _NodeLabelsStore_38(CommonNodeLabelsManager baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Recover()
			{
			}

			// do nothing
			/// <exception cref="System.IO.IOException"/>
			public override void RemoveClusterNodeLabels(ICollection<string> labels)
			{
			}

			// do nothing
			/// <exception cref="System.IO.IOException"/>
			public override void UpdateNodeToLabelsMappings(IDictionary<NodeId, ICollection<string
				>> nodeToLabels)
			{
			}

			// do nothing
			/// <exception cref="System.IO.IOException"/>
			public override void StoreNewClusterNodeLabels(ICollection<string> label)
			{
			}

			// do nothing
			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}
		}

		// do nothing
		protected override void InitDispatcher(Configuration conf)
		{
			base.dispatcher = null;
		}

		protected override void StartDispatcher()
		{
		}

		// do nothing
		protected override void StopDispatcher()
		{
		}

		// do nothing
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			// always enable node labels while using MemoryRMNodeLabelsManager
			conf.SetBoolean(YarnConfiguration.NodeLabelsEnabled, true);
			base.ServiceInit(conf);
		}
	}
}
