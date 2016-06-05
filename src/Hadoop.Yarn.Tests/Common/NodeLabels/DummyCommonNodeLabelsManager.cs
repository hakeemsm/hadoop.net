using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels
{
	public class DummyCommonNodeLabelsManager : CommonNodeLabelsManager
	{
		internal IDictionary<NodeId, ICollection<string>> lastNodeToLabels = null;

		internal ICollection<string> lastAddedlabels = null;

		internal ICollection<string> lastRemovedlabels = null;

		protected internal override void InitNodeLabelStore(Configuration conf)
		{
			this.store = new _NodeLabelsStore_37(this, this);
		}

		private sealed class _NodeLabelsStore_37 : NodeLabelsStore
		{
			public _NodeLabelsStore_37(DummyCommonNodeLabelsManager _enclosing, CommonNodeLabelsManager
				 baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Recover()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveClusterNodeLabels(ICollection<string> labels)
			{
				this._enclosing.lastRemovedlabels = labels;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void UpdateNodeToLabelsMappings(IDictionary<NodeId, ICollection<string
				>> nodeToLabels)
			{
				this._enclosing.lastNodeToLabels = nodeToLabels;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void StoreNewClusterNodeLabels(ICollection<string> label)
			{
				this._enclosing.lastAddedlabels = label;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}

			private readonly DummyCommonNodeLabelsManager _enclosing;
		}

		// do nothing 
		protected internal override void InitDispatcher(Configuration conf)
		{
			base.dispatcher = new InlineDispatcher();
		}

		protected internal override void StartDispatcher()
		{
		}

		// do nothing
		protected internal override void StopDispatcher()
		{
		}

		// do nothing
		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			base.ServiceStop();
		}
	}
}
