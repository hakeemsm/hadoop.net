using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class AllocateRequestPBImpl : AllocateRequest
	{
		internal YarnServiceProtos.AllocateRequestProto proto = YarnServiceProtos.AllocateRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.AllocateRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private IList<ResourceRequest> ask = null;

		private IList<ContainerId> release = null;

		private IList<ContainerResourceIncreaseRequest> increaseRequests = null;

		private ResourceBlacklistRequest blacklistRequest = null;

		public AllocateRequestPBImpl()
		{
			builder = YarnServiceProtos.AllocateRequestProto.NewBuilder();
		}

		public AllocateRequestPBImpl(YarnServiceProtos.AllocateRequestProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.AllocateRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.AllocateRequestProto)builder.Build
				());
			viaProto = true;
			return proto;
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
			if (this.ask != null)
			{
				AddAsksToProto();
			}
			if (this.release != null)
			{
				AddReleasesToProto();
			}
			if (this.increaseRequests != null)
			{
				AddIncreaseRequestsToProto();
			}
			if (this.blacklistRequest != null)
			{
				builder.SetBlacklistRequest(ConvertToProtoFormat(this.blacklistRequest));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.AllocateRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.AllocateRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override int GetResponseId()
		{
			YarnServiceProtos.AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetResponseId();
		}

		public override void SetResponseId(int id)
		{
			MaybeInitBuilder();
			builder.SetResponseId(id);
		}

		public override float GetProgress()
		{
			YarnServiceProtos.AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetProgress();
		}

		public override void SetProgress(float progress)
		{
			MaybeInitBuilder();
			builder.SetProgress(progress);
		}

		public override IList<ResourceRequest> GetAskList()
		{
			InitAsks();
			return this.ask;
		}

		public override void SetAskList(IList<ResourceRequest> resourceRequests)
		{
			if (resourceRequests == null)
			{
				return;
			}
			InitAsks();
			this.ask.Clear();
			Sharpen.Collections.AddAll(this.ask, resourceRequests);
		}

		public override IList<ContainerResourceIncreaseRequest> GetIncreaseRequests()
		{
			InitIncreaseRequests();
			return this.increaseRequests;
		}

		public override void SetIncreaseRequests(IList<ContainerResourceIncreaseRequest> 
			increaseRequests)
		{
			if (increaseRequests == null)
			{
				return;
			}
			InitIncreaseRequests();
			this.increaseRequests.Clear();
			Sharpen.Collections.AddAll(this.increaseRequests, increaseRequests);
		}

		public override ResourceBlacklistRequest GetResourceBlacklistRequest()
		{
			YarnServiceProtos.AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.blacklistRequest != null)
			{
				return this.blacklistRequest;
			}
			if (!p.HasBlacklistRequest())
			{
				return null;
			}
			this.blacklistRequest = ConvertFromProtoFormat(p.GetBlacklistRequest());
			return this.blacklistRequest;
		}

		public override void SetResourceBlacklistRequest(ResourceBlacklistRequest blacklistRequest
			)
		{
			MaybeInitBuilder();
			if (blacklistRequest == null)
			{
				builder.ClearBlacklistRequest();
			}
			this.blacklistRequest = blacklistRequest;
		}

		private void InitAsks()
		{
			if (this.ask != null)
			{
				return;
			}
			YarnServiceProtos.AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ResourceRequestProto> list = p.GetAskList();
			this.ask = new AList<ResourceRequest>();
			foreach (YarnProtos.ResourceRequestProto c in list)
			{
				this.ask.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void AddAsksToProto()
		{
			MaybeInitBuilder();
			builder.ClearAsk();
			if (ask == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ResourceRequestProto> iterable = new _IEnumerable_221(this
				);
			builder.AddAllAsk(iterable);
		}

		private sealed class _IEnumerable_221 : IEnumerable<YarnProtos.ResourceRequestProto
			>
		{
			public _IEnumerable_221(AllocateRequestPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ResourceRequestProto> GetEnumerator()
			{
				return new _IEnumerator_224(this);
			}

			private sealed class _IEnumerator_224 : IEnumerator<YarnProtos.ResourceRequestProto
				>
			{
				public _IEnumerator_224(_IEnumerable_221 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.ask.GetEnumerator();
				}

				internal IEnumerator<ResourceRequest> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ResourceRequestProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_221 _enclosing;
			}

			private readonly AllocateRequestPBImpl _enclosing;
		}

		private void InitIncreaseRequests()
		{
			if (this.increaseRequests != null)
			{
				return;
			}
			YarnServiceProtos.AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ContainerResourceIncreaseRequestProto> list = p.GetIncreaseRequestList
				();
			this.increaseRequests = new AList<ContainerResourceIncreaseRequest>();
			foreach (YarnProtos.ContainerResourceIncreaseRequestProto c in list)
			{
				this.increaseRequests.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void AddIncreaseRequestsToProto()
		{
			MaybeInitBuilder();
			builder.ClearIncreaseRequest();
			if (increaseRequests == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ContainerResourceIncreaseRequestProto> iterable = new _IEnumerable_271
				(this);
			builder.AddAllIncreaseRequest(iterable);
		}

		private sealed class _IEnumerable_271 : IEnumerable<YarnProtos.ContainerResourceIncreaseRequestProto
			>
		{
			public _IEnumerable_271(AllocateRequestPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ContainerResourceIncreaseRequestProto> GetEnumerator
				()
			{
				return new _IEnumerator_274(this);
			}

			private sealed class _IEnumerator_274 : IEnumerator<YarnProtos.ContainerResourceIncreaseRequestProto
				>
			{
				public _IEnumerator_274(_IEnumerable_271 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.increaseRequests.GetEnumerator();
				}

				internal IEnumerator<ContainerResourceIncreaseRequest> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ContainerResourceIncreaseRequestProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_271 _enclosing;
			}

			private readonly AllocateRequestPBImpl _enclosing;
		}

		public override IList<ContainerId> GetReleaseList()
		{
			InitReleases();
			return this.release;
		}

		public override void SetReleaseList(IList<ContainerId> releaseContainers)
		{
			if (releaseContainers == null)
			{
				return;
			}
			InitReleases();
			this.release.Clear();
			Sharpen.Collections.AddAll(this.release, releaseContainers);
		}

		private void InitReleases()
		{
			if (this.release != null)
			{
				return;
			}
			YarnServiceProtos.AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ContainerIdProto> list = p.GetReleaseList();
			this.release = new AList<ContainerId>();
			foreach (YarnProtos.ContainerIdProto c in list)
			{
				this.release.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void AddReleasesToProto()
		{
			MaybeInitBuilder();
			builder.ClearRelease();
			if (release == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ContainerIdProto> iterable = new _IEnumerable_333(this);
			builder.AddAllRelease(iterable);
		}

		private sealed class _IEnumerable_333 : IEnumerable<YarnProtos.ContainerIdProto>
		{
			public _IEnumerable_333(AllocateRequestPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ContainerIdProto> GetEnumerator()
			{
				return new _IEnumerator_336(this);
			}

			private sealed class _IEnumerator_336 : IEnumerator<YarnProtos.ContainerIdProto>
			{
				public _IEnumerator_336(_IEnumerable_333 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.release.GetEnumerator();
				}

				internal IEnumerator<ContainerId> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ContainerIdProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_333 _enclosing;
			}

			private readonly AllocateRequestPBImpl _enclosing;
		}

		private ResourceRequestPBImpl ConvertFromProtoFormat(YarnProtos.ResourceRequestProto
			 p)
		{
			return new ResourceRequestPBImpl(p);
		}

		private YarnProtos.ResourceRequestProto ConvertToProtoFormat(ResourceRequest t)
		{
			return ((ResourceRequestPBImpl)t).GetProto();
		}

		private ContainerResourceIncreaseRequestPBImpl ConvertFromProtoFormat(YarnProtos.ContainerResourceIncreaseRequestProto
			 p)
		{
			return new ContainerResourceIncreaseRequestPBImpl(p);
		}

		private YarnProtos.ContainerResourceIncreaseRequestProto ConvertToProtoFormat(ContainerResourceIncreaseRequest
			 t)
		{
			return ((ContainerResourceIncreaseRequestPBImpl)t).GetProto();
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}

		private ResourceBlacklistRequestPBImpl ConvertFromProtoFormat(YarnProtos.ResourceBlacklistRequestProto
			 p)
		{
			return new ResourceBlacklistRequestPBImpl(p);
		}

		private YarnProtos.ResourceBlacklistRequestProto ConvertToProtoFormat(ResourceBlacklistRequest
			 t)
		{
			return ((ResourceBlacklistRequestPBImpl)t).GetProto();
		}
	}
}
