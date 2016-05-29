using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class AllocateResponsePBImpl : AllocateResponse
	{
		internal YarnServiceProtos.AllocateResponseProto proto = YarnServiceProtos.AllocateResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.AllocateResponseProto.Builder builder = null;

		internal bool viaProto = false;

		internal Resource limit;

		private IList<Container> allocatedContainers = null;

		private IList<NMToken> nmTokens = null;

		private IList<ContainerStatus> completedContainersStatuses = null;

		private IList<ContainerResourceIncrease> increasedContainers = null;

		private IList<ContainerResourceDecrease> decreasedContainers = null;

		private IList<NodeReport> updatedNodes = null;

		private PreemptionMessage preempt;

		private Token amrmToken = null;

		public AllocateResponsePBImpl()
		{
			builder = YarnServiceProtos.AllocateResponseProto.NewBuilder();
		}

		public AllocateResponsePBImpl(YarnServiceProtos.AllocateResponseProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.AllocateResponseProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnServiceProtos.AllocateResponseProto)builder.Build
					());
				viaProto = true;
				return proto;
			}
		}

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetProto().Equals(this.GetType().Cast(other).GetProto());
			}
			return false;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetProto());
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.allocatedContainers != null)
				{
					builder.ClearAllocatedContainers();
					IEnumerable<YarnProtos.ContainerProto> iterable = GetContainerProtoIterable(this.
						allocatedContainers);
					builder.AddAllAllocatedContainers(iterable);
				}
				if (nmTokens != null)
				{
					builder.ClearNmTokens();
					IEnumerable<YarnServiceProtos.NMTokenProto> iterable = GetTokenProtoIterable(nmTokens
						);
					builder.AddAllNmTokens(iterable);
				}
				if (this.completedContainersStatuses != null)
				{
					builder.ClearCompletedContainerStatuses();
					IEnumerable<YarnProtos.ContainerStatusProto> iterable = GetContainerStatusProtoIterable
						(this.completedContainersStatuses);
					builder.AddAllCompletedContainerStatuses(iterable);
				}
				if (this.updatedNodes != null)
				{
					builder.ClearUpdatedNodes();
					IEnumerable<YarnProtos.NodeReportProto> iterable = GetNodeReportProtoIterable(this
						.updatedNodes);
					builder.AddAllUpdatedNodes(iterable);
				}
				if (this.limit != null)
				{
					builder.SetLimit(ConvertToProtoFormat(this.limit));
				}
				if (this.preempt != null)
				{
					builder.SetPreempt(ConvertToProtoFormat(this.preempt));
				}
				if (this.increasedContainers != null)
				{
					builder.ClearIncreasedContainers();
					IEnumerable<YarnProtos.ContainerResourceIncreaseProto> iterable = GetIncreaseProtoIterable
						(this.increasedContainers);
					builder.AddAllIncreasedContainers(iterable);
				}
				if (this.decreasedContainers != null)
				{
					builder.ClearDecreasedContainers();
					IEnumerable<YarnProtos.ContainerResourceDecreaseProto> iterable = GetChangeProtoIterable
						(this.decreasedContainers);
					builder.AddAllDecreasedContainers(iterable);
				}
				if (this.amrmToken != null)
				{
					builder.SetAmRmToken(ConvertToProtoFormat(this.amrmToken));
				}
			}
		}

		private void MergeLocalToProto()
		{
			lock (this)
			{
				if (viaProto)
				{
					MaybeInitBuilder();
				}
				MergeLocalToBuilder();
				proto = ((YarnServiceProtos.AllocateResponseProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = YarnServiceProtos.AllocateResponseProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public override AMCommand GetAMCommand()
		{
			lock (this)
			{
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasAMCommand())
				{
					return null;
				}
				return ProtoUtils.ConvertFromProtoFormat(p.GetAMCommand());
			}
		}

		public override void SetAMCommand(AMCommand command)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (command == null)
				{
					builder.ClearAMCommand();
					return;
				}
				builder.SetAMCommand(ProtoUtils.ConvertToProtoFormat(command));
			}
		}

		public override int GetResponseId()
		{
			lock (this)
			{
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetResponseId());
			}
		}

		public override void SetResponseId(int responseId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetResponseId((responseId));
			}
		}

		public override Resource GetAvailableResources()
		{
			lock (this)
			{
				if (this.limit != null)
				{
					return this.limit;
				}
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasLimit())
				{
					return null;
				}
				this.limit = ConvertFromProtoFormat(p.GetLimit());
				return this.limit;
			}
		}

		public override void SetAvailableResources(Resource limit)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (limit == null)
				{
					builder.ClearLimit();
				}
				this.limit = limit;
			}
		}

		public override IList<NodeReport> GetUpdatedNodes()
		{
			lock (this)
			{
				InitLocalNewNodeReportList();
				return this.updatedNodes;
			}
		}

		public override void SetUpdatedNodes(IList<NodeReport> updatedNodes)
		{
			lock (this)
			{
				if (updatedNodes == null)
				{
					this.updatedNodes.Clear();
					return;
				}
				this.updatedNodes = new AList<NodeReport>(updatedNodes.Count);
				Sharpen.Collections.AddAll(this.updatedNodes, updatedNodes);
			}
		}

		public override IList<Container> GetAllocatedContainers()
		{
			lock (this)
			{
				InitLocalNewContainerList();
				return this.allocatedContainers;
			}
		}

		public override void SetAllocatedContainers(IList<Container> containers)
		{
			lock (this)
			{
				if (containers == null)
				{
					return;
				}
				// this looks like a bug because it results in append and not set
				InitLocalNewContainerList();
				Sharpen.Collections.AddAll(allocatedContainers, containers);
			}
		}

		//// Finished containers
		public override IList<ContainerStatus> GetCompletedContainersStatuses()
		{
			lock (this)
			{
				InitLocalFinishedContainerList();
				return this.completedContainersStatuses;
			}
		}

		public override void SetCompletedContainersStatuses(IList<ContainerStatus> containers
			)
		{
			lock (this)
			{
				if (containers == null)
				{
					return;
				}
				InitLocalFinishedContainerList();
				Sharpen.Collections.AddAll(completedContainersStatuses, containers);
			}
		}

		public override void SetNMTokens(IList<NMToken> nmTokens)
		{
			lock (this)
			{
				if (nmTokens == null || nmTokens.IsEmpty())
				{
					if (this.nmTokens != null)
					{
						this.nmTokens.Clear();
					}
					builder.ClearNmTokens();
					return;
				}
				// Implementing it as an append rather than set for consistency
				InitLocalNewNMTokenList();
				Sharpen.Collections.AddAll(this.nmTokens, nmTokens);
			}
		}

		public override IList<NMToken> GetNMTokens()
		{
			lock (this)
			{
				InitLocalNewNMTokenList();
				return nmTokens;
			}
		}

		public override int GetNumClusterNodes()
		{
			lock (this)
			{
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				return p.GetNumClusterNodes();
			}
		}

		public override void SetNumClusterNodes(int numNodes)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetNumClusterNodes(numNodes);
			}
		}

		public override PreemptionMessage GetPreemptionMessage()
		{
			lock (this)
			{
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				if (this.preempt != null)
				{
					return this.preempt;
				}
				if (!p.HasPreempt())
				{
					return null;
				}
				this.preempt = ConvertFromProtoFormat(p.GetPreempt());
				return this.preempt;
			}
		}

		public override void SetPreemptionMessage(PreemptionMessage preempt)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (null == preempt)
				{
					builder.ClearPreempt();
				}
				this.preempt = preempt;
			}
		}

		public override IList<ContainerResourceIncrease> GetIncreasedContainers()
		{
			lock (this)
			{
				InitLocalIncreasedContainerList();
				return increasedContainers;
			}
		}

		public override void SetIncreasedContainers(IList<ContainerResourceIncrease> increasedContainers
			)
		{
			lock (this)
			{
				if (increasedContainers == null)
				{
					return;
				}
				InitLocalIncreasedContainerList();
				Sharpen.Collections.AddAll(this.increasedContainers, increasedContainers);
			}
		}

		public override IList<ContainerResourceDecrease> GetDecreasedContainers()
		{
			lock (this)
			{
				InitLocalDecreasedContainerList();
				return decreasedContainers;
			}
		}

		public override void SetDecreasedContainers(IList<ContainerResourceDecrease> decreasedContainers
			)
		{
			lock (this)
			{
				if (decreasedContainers == null)
				{
					return;
				}
				InitLocalDecreasedContainerList();
				Sharpen.Collections.AddAll(this.decreasedContainers, decreasedContainers);
			}
		}

		public override Token GetAMRMToken()
		{
			lock (this)
			{
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				if (amrmToken != null)
				{
					return amrmToken;
				}
				if (!p.HasAmRmToken())
				{
					return null;
				}
				this.amrmToken = ConvertFromProtoFormat(p.GetAmRmToken());
				return amrmToken;
			}
		}

		public override void SetAMRMToken(Token amRMToken)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (amRMToken == null)
				{
					builder.ClearAmRmToken();
				}
				this.amrmToken = amRMToken;
			}
		}

		private void InitLocalIncreasedContainerList()
		{
			lock (this)
			{
				if (this.increasedContainers != null)
				{
					return;
				}
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				IList<YarnProtos.ContainerResourceIncreaseProto> list = p.GetIncreasedContainersList
					();
				increasedContainers = new AList<ContainerResourceIncrease>();
				foreach (YarnProtos.ContainerResourceIncreaseProto c in list)
				{
					increasedContainers.AddItem(ConvertFromProtoFormat(c));
				}
			}
		}

		private void InitLocalDecreasedContainerList()
		{
			lock (this)
			{
				if (this.decreasedContainers != null)
				{
					return;
				}
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				IList<YarnProtos.ContainerResourceDecreaseProto> list = p.GetDecreasedContainersList
					();
				decreasedContainers = new AList<ContainerResourceDecrease>();
				foreach (YarnProtos.ContainerResourceDecreaseProto c in list)
				{
					decreasedContainers.AddItem(ConvertFromProtoFormat(c));
				}
			}
		}

		// Once this is called. updatedNodes will never be null - until a getProto is
		// called.
		private void InitLocalNewNodeReportList()
		{
			lock (this)
			{
				if (this.updatedNodes != null)
				{
					return;
				}
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				IList<YarnProtos.NodeReportProto> list = p.GetUpdatedNodesList();
				updatedNodes = new AList<NodeReport>(list.Count);
				foreach (YarnProtos.NodeReportProto n in list)
				{
					updatedNodes.AddItem(ConvertFromProtoFormat(n));
				}
			}
		}

		// Once this is called. containerList will never be null - until a getProto
		// is called.
		private void InitLocalNewContainerList()
		{
			lock (this)
			{
				if (this.allocatedContainers != null)
				{
					return;
				}
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				IList<YarnProtos.ContainerProto> list = p.GetAllocatedContainersList();
				allocatedContainers = new AList<Container>();
				foreach (YarnProtos.ContainerProto c in list)
				{
					allocatedContainers.AddItem(ConvertFromProtoFormat(c));
				}
			}
		}

		private void InitLocalNewNMTokenList()
		{
			lock (this)
			{
				if (nmTokens != null)
				{
					return;
				}
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				IList<YarnServiceProtos.NMTokenProto> list = p.GetNmTokensList();
				nmTokens = new AList<NMToken>();
				foreach (YarnServiceProtos.NMTokenProto t in list)
				{
					nmTokens.AddItem(ConvertFromProtoFormat(t));
				}
			}
		}

		private IEnumerable<YarnProtos.ContainerResourceIncreaseProto> GetIncreaseProtoIterable
			(IList<ContainerResourceIncrease> newContainersList)
		{
			lock (this)
			{
				MaybeInitBuilder();
				return new _IEnumerable_460(this, newContainersList);
			}
		}

		private sealed class _IEnumerable_460 : IEnumerable<YarnProtos.ContainerResourceIncreaseProto
			>
		{
			public _IEnumerable_460(AllocateResponsePBImpl _enclosing, IList<ContainerResourceIncrease
				> newContainersList)
			{
				this._enclosing = _enclosing;
				this.newContainersList = newContainersList;
			}

			public override IEnumerator<YarnProtos.ContainerResourceIncreaseProto> GetEnumerator
				()
			{
				lock (this)
				{
					return new _IEnumerator_463(this, newContainersList);
				}
			}

			private sealed class _IEnumerator_463 : IEnumerator<YarnProtos.ContainerResourceIncreaseProto
				>
			{
				public _IEnumerator_463(_IEnumerable_460 _enclosing, IList<ContainerResourceIncrease
					> newContainersList)
				{
					this._enclosing = _enclosing;
					this.newContainersList = newContainersList;
					this.iter = newContainersList.GetEnumerator();
				}

				internal IEnumerator<ContainerResourceIncrease> iter;

				public override bool HasNext()
				{
					lock (this)
					{
						return this.iter.HasNext();
					}
				}

				public override YarnProtos.ContainerResourceIncreaseProto Next()
				{
					lock (this)
					{
						return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
					}
				}

				public override void Remove()
				{
					lock (this)
					{
						throw new NotSupportedException();
					}
				}

				private readonly _IEnumerable_460 _enclosing;

				private readonly IList<ContainerResourceIncrease> newContainersList;
			}

			private readonly AllocateResponsePBImpl _enclosing;

			private readonly IList<ContainerResourceIncrease> newContainersList;
		}

		private IEnumerable<YarnProtos.ContainerResourceDecreaseProto> GetChangeProtoIterable
			(IList<ContainerResourceDecrease> newContainersList)
		{
			lock (this)
			{
				MaybeInitBuilder();
				return new _IEnumerable_492(this, newContainersList);
			}
		}

		private sealed class _IEnumerable_492 : IEnumerable<YarnProtos.ContainerResourceDecreaseProto
			>
		{
			public _IEnumerable_492(AllocateResponsePBImpl _enclosing, IList<ContainerResourceDecrease
				> newContainersList)
			{
				this._enclosing = _enclosing;
				this.newContainersList = newContainersList;
			}

			public override IEnumerator<YarnProtos.ContainerResourceDecreaseProto> GetEnumerator
				()
			{
				lock (this)
				{
					return new _IEnumerator_495(this, newContainersList);
				}
			}

			private sealed class _IEnumerator_495 : IEnumerator<YarnProtos.ContainerResourceDecreaseProto
				>
			{
				public _IEnumerator_495(_IEnumerable_492 _enclosing, IList<ContainerResourceDecrease
					> newContainersList)
				{
					this._enclosing = _enclosing;
					this.newContainersList = newContainersList;
					this.iter = newContainersList.GetEnumerator();
				}

				internal IEnumerator<ContainerResourceDecrease> iter;

				public override bool HasNext()
				{
					lock (this)
					{
						return this.iter.HasNext();
					}
				}

				public override YarnProtos.ContainerResourceDecreaseProto Next()
				{
					lock (this)
					{
						return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
					}
				}

				public override void Remove()
				{
					lock (this)
					{
						throw new NotSupportedException();
					}
				}

				private readonly _IEnumerable_492 _enclosing;

				private readonly IList<ContainerResourceDecrease> newContainersList;
			}

			private readonly AllocateResponsePBImpl _enclosing;

			private readonly IList<ContainerResourceDecrease> newContainersList;
		}

		private IEnumerable<YarnProtos.ContainerProto> GetContainerProtoIterable(IList<Container
			> newContainersList)
		{
			lock (this)
			{
				MaybeInitBuilder();
				return new _IEnumerable_523(this, newContainersList);
			}
		}

		private sealed class _IEnumerable_523 : IEnumerable<YarnProtos.ContainerProto>
		{
			public _IEnumerable_523(AllocateResponsePBImpl _enclosing, IList<Container> newContainersList
				)
			{
				this._enclosing = _enclosing;
				this.newContainersList = newContainersList;
			}

			public override IEnumerator<YarnProtos.ContainerProto> GetEnumerator()
			{
				lock (this)
				{
					return new _IEnumerator_526(this, newContainersList);
				}
			}

			private sealed class _IEnumerator_526 : IEnumerator<YarnProtos.ContainerProto>
			{
				public _IEnumerator_526(_IEnumerable_523 _enclosing, IList<Container> newContainersList
					)
				{
					this._enclosing = _enclosing;
					this.newContainersList = newContainersList;
					this.iter = newContainersList.GetEnumerator();
				}

				internal IEnumerator<Container> iter;

				public override bool HasNext()
				{
					lock (this)
					{
						return this.iter.HasNext();
					}
				}

				public override YarnProtos.ContainerProto Next()
				{
					lock (this)
					{
						return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
					}
				}

				public override void Remove()
				{
					lock (this)
					{
						throw new NotSupportedException();
					}
				}

				private readonly _IEnumerable_523 _enclosing;

				private readonly IList<Container> newContainersList;
			}

			private readonly AllocateResponsePBImpl _enclosing;

			private readonly IList<Container> newContainersList;
		}

		private IEnumerable<YarnServiceProtos.NMTokenProto> GetTokenProtoIterable(IList<NMToken
			> nmTokenList)
		{
			lock (this)
			{
				MaybeInitBuilder();
				return new _IEnumerable_554(this, nmTokenList);
			}
		}

		private sealed class _IEnumerable_554 : IEnumerable<YarnServiceProtos.NMTokenProto
			>
		{
			public _IEnumerable_554(AllocateResponsePBImpl _enclosing, IList<NMToken> nmTokenList
				)
			{
				this._enclosing = _enclosing;
				this.nmTokenList = nmTokenList;
			}

			public override IEnumerator<YarnServiceProtos.NMTokenProto> GetEnumerator()
			{
				lock (this)
				{
					return new _IEnumerator_557(this, nmTokenList);
				}
			}

			private sealed class _IEnumerator_557 : IEnumerator<YarnServiceProtos.NMTokenProto
				>
			{
				public _IEnumerator_557(_IEnumerable_554 _enclosing, IList<NMToken> nmTokenList)
				{
					this._enclosing = _enclosing;
					this.nmTokenList = nmTokenList;
					this.iter = nmTokenList.GetEnumerator();
				}

				internal IEnumerator<NMToken> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnServiceProtos.NMTokenProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_554 _enclosing;

				private readonly IList<NMToken> nmTokenList;
			}

			private readonly AllocateResponsePBImpl _enclosing;

			private readonly IList<NMToken> nmTokenList;
		}

		private IEnumerable<YarnProtos.ContainerStatusProto> GetContainerStatusProtoIterable
			(IList<ContainerStatus> newContainersList)
		{
			lock (this)
			{
				MaybeInitBuilder();
				return new _IEnumerable_584(this, newContainersList);
			}
		}

		private sealed class _IEnumerable_584 : IEnumerable<YarnProtos.ContainerStatusProto
			>
		{
			public _IEnumerable_584(AllocateResponsePBImpl _enclosing, IList<ContainerStatus>
				 newContainersList)
			{
				this._enclosing = _enclosing;
				this.newContainersList = newContainersList;
			}

			public override IEnumerator<YarnProtos.ContainerStatusProto> GetEnumerator()
			{
				lock (this)
				{
					return new _IEnumerator_587(this, newContainersList);
				}
			}

			private sealed class _IEnumerator_587 : IEnumerator<YarnProtos.ContainerStatusProto
				>
			{
				public _IEnumerator_587(_IEnumerable_584 _enclosing, IList<ContainerStatus> newContainersList
					)
				{
					this._enclosing = _enclosing;
					this.newContainersList = newContainersList;
					this.iter = newContainersList.GetEnumerator();
				}

				internal IEnumerator<ContainerStatus> iter;

				public override bool HasNext()
				{
					lock (this)
					{
						return this.iter.HasNext();
					}
				}

				public override YarnProtos.ContainerStatusProto Next()
				{
					lock (this)
					{
						return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
					}
				}

				public override void Remove()
				{
					lock (this)
					{
						throw new NotSupportedException();
					}
				}

				private readonly _IEnumerable_584 _enclosing;

				private readonly IList<ContainerStatus> newContainersList;
			}

			private readonly AllocateResponsePBImpl _enclosing;

			private readonly IList<ContainerStatus> newContainersList;
		}

		private IEnumerable<YarnProtos.NodeReportProto> GetNodeReportProtoIterable(IList<
			NodeReport> newNodeReportsList)
		{
			lock (this)
			{
				MaybeInitBuilder();
				return new _IEnumerable_616(this, newNodeReportsList);
			}
		}

		private sealed class _IEnumerable_616 : IEnumerable<YarnProtos.NodeReportProto>
		{
			public _IEnumerable_616(AllocateResponsePBImpl _enclosing, IList<NodeReport> newNodeReportsList
				)
			{
				this._enclosing = _enclosing;
				this.newNodeReportsList = newNodeReportsList;
			}

			public override IEnumerator<YarnProtos.NodeReportProto> GetEnumerator()
			{
				lock (this)
				{
					return new _IEnumerator_619(this, newNodeReportsList);
				}
			}

			private sealed class _IEnumerator_619 : IEnumerator<YarnProtos.NodeReportProto>
			{
				public _IEnumerator_619(_IEnumerable_616 _enclosing, IList<NodeReport> newNodeReportsList
					)
				{
					this._enclosing = _enclosing;
					this.newNodeReportsList = newNodeReportsList;
					this.iter = newNodeReportsList.GetEnumerator();
				}

				internal IEnumerator<NodeReport> iter;

				public override bool HasNext()
				{
					lock (this)
					{
						return this.iter.HasNext();
					}
				}

				public override YarnProtos.NodeReportProto Next()
				{
					lock (this)
					{
						return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
					}
				}

				public override void Remove()
				{
					lock (this)
					{
						throw new NotSupportedException();
					}
				}

				private readonly _IEnumerable_616 _enclosing;

				private readonly IList<NodeReport> newNodeReportsList;
			}

			private readonly AllocateResponsePBImpl _enclosing;

			private readonly IList<NodeReport> newNodeReportsList;
		}

		// Once this is called. containerList will never be null - until a getProto
		// is called.
		private void InitLocalFinishedContainerList()
		{
			lock (this)
			{
				if (this.completedContainersStatuses != null)
				{
					return;
				}
				YarnServiceProtos.AllocateResponseProtoOrBuilder p = viaProto ? proto : builder;
				IList<YarnProtos.ContainerStatusProto> list = p.GetCompletedContainerStatusesList
					();
				completedContainersStatuses = new AList<ContainerStatus>();
				foreach (YarnProtos.ContainerStatusProto c in list)
				{
					completedContainersStatuses.AddItem(ConvertFromProtoFormat(c));
				}
			}
		}

		private ContainerResourceIncrease ConvertFromProtoFormat(YarnProtos.ContainerResourceIncreaseProto
			 p)
		{
			lock (this)
			{
				return new ContainerResourceIncreasePBImpl(p);
			}
		}

		private YarnProtos.ContainerResourceIncreaseProto ConvertToProtoFormat(ContainerResourceIncrease
			 t)
		{
			lock (this)
			{
				return ((ContainerResourceIncreasePBImpl)t).GetProto();
			}
		}

		private ContainerResourceDecrease ConvertFromProtoFormat(YarnProtos.ContainerResourceDecreaseProto
			 p)
		{
			lock (this)
			{
				return new ContainerResourceDecreasePBImpl(p);
			}
		}

		private YarnProtos.ContainerResourceDecreaseProto ConvertToProtoFormat(ContainerResourceDecrease
			 t)
		{
			lock (this)
			{
				return ((ContainerResourceDecreasePBImpl)t).GetProto();
			}
		}

		private NodeReportPBImpl ConvertFromProtoFormat(YarnProtos.NodeReportProto p)
		{
			lock (this)
			{
				return new NodeReportPBImpl(p);
			}
		}

		private YarnProtos.NodeReportProto ConvertToProtoFormat(NodeReport t)
		{
			lock (this)
			{
				return ((NodeReportPBImpl)t).GetProto();
			}
		}

		private ContainerPBImpl ConvertFromProtoFormat(YarnProtos.ContainerProto p)
		{
			lock (this)
			{
				return new ContainerPBImpl(p);
			}
		}

		private YarnProtos.ContainerProto ConvertToProtoFormat(Container t)
		{
			lock (this)
			{
				return ((ContainerPBImpl)t).GetProto();
			}
		}

		private ContainerStatusPBImpl ConvertFromProtoFormat(YarnProtos.ContainerStatusProto
			 p)
		{
			lock (this)
			{
				return new ContainerStatusPBImpl(p);
			}
		}

		private YarnProtos.ContainerStatusProto ConvertToProtoFormat(ContainerStatus t)
		{
			lock (this)
			{
				return ((ContainerStatusPBImpl)t).GetProto();
			}
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			lock (this)
			{
				return new ResourcePBImpl(p);
			}
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource r)
		{
			lock (this)
			{
				return ((ResourcePBImpl)r).GetProto();
			}
		}

		private PreemptionMessagePBImpl ConvertFromProtoFormat(YarnProtos.PreemptionMessageProto
			 p)
		{
			lock (this)
			{
				return new PreemptionMessagePBImpl(p);
			}
		}

		private YarnProtos.PreemptionMessageProto ConvertToProtoFormat(PreemptionMessage 
			r)
		{
			lock (this)
			{
				return ((PreemptionMessagePBImpl)r).GetProto();
			}
		}

		private YarnServiceProtos.NMTokenProto ConvertToProtoFormat(NMToken token)
		{
			lock (this)
			{
				return ((NMTokenPBImpl)token).GetProto();
			}
		}

		private NMToken ConvertFromProtoFormat(YarnServiceProtos.NMTokenProto proto)
		{
			lock (this)
			{
				return new NMTokenPBImpl(proto);
			}
		}

		private TokenPBImpl ConvertFromProtoFormat(SecurityProtos.TokenProto p)
		{
			return new TokenPBImpl(p);
		}

		private SecurityProtos.TokenProto ConvertToProtoFormat(Token t)
		{
			return ((TokenPBImpl)t).GetProto();
		}
	}
}
