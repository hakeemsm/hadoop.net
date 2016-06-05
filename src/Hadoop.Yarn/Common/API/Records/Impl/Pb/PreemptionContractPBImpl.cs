using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class PreemptionContractPBImpl : PreemptionContract
	{
		internal YarnProtos.PreemptionContractProto proto = YarnProtos.PreemptionContractProto
			.GetDefaultInstance();

		internal YarnProtos.PreemptionContractProto.Builder builder = null;

		internal bool viaProto = false;

		private ICollection<PreemptionContainer> containers;

		private IList<PreemptionResourceRequest> resources;

		public PreemptionContractPBImpl()
		{
			builder = YarnProtos.PreemptionContractProto.NewBuilder();
		}

		public PreemptionContractPBImpl(YarnProtos.PreemptionContractProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.PreemptionContractProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnProtos.PreemptionContractProto)builder.Build());
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

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.PreemptionContractProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (this.resources != null)
			{
				AddResourcesToProto();
			}
			if (this.containers != null)
			{
				AddContainersToProto();
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.PreemptionContractProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ICollection<PreemptionContainer> GetContainers()
		{
			lock (this)
			{
				InitPreemptionContainers();
				return containers;
			}
		}

		public override void SetContainers(ICollection<PreemptionContainer> containers)
		{
			lock (this)
			{
				if (null == containers)
				{
					builder.ClearContainer();
				}
				this.containers = containers;
			}
		}

		public override IList<PreemptionResourceRequest> GetResourceRequest()
		{
			lock (this)
			{
				InitPreemptionResourceRequests();
				return resources;
			}
		}

		public override void SetResourceRequest(IList<PreemptionResourceRequest> req)
		{
			lock (this)
			{
				if (null == resources)
				{
					builder.ClearResource();
				}
				this.resources = req;
			}
		}

		private void InitPreemptionResourceRequests()
		{
			if (resources != null)
			{
				return;
			}
			YarnProtos.PreemptionContractProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.PreemptionResourceRequestProto> list = p.GetResourceList();
			resources = new AList<PreemptionResourceRequest>();
			foreach (YarnProtos.PreemptionResourceRequestProto rr in list)
			{
				resources.AddItem(ConvertFromProtoFormat(rr));
			}
		}

		private void AddResourcesToProto()
		{
			MaybeInitBuilder();
			builder.ClearResource();
			if (null == resources)
			{
				return;
			}
			IEnumerable<YarnProtos.PreemptionResourceRequestProto> iterable = new _IEnumerable_159
				(this);
			builder.AddAllResource(iterable);
		}

		private sealed class _IEnumerable_159 : IEnumerable<YarnProtos.PreemptionResourceRequestProto
			>
		{
			public _IEnumerable_159(PreemptionContractPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.PreemptionResourceRequestProto> GetEnumerator
				()
			{
				return new _IEnumerator_162(this);
			}

			private sealed class _IEnumerator_162 : IEnumerator<YarnProtos.PreemptionResourceRequestProto
				>
			{
				public _IEnumerator_162(_IEnumerable_159 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.resources.GetEnumerator();
				}

				internal IEnumerator<PreemptionResourceRequest> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.PreemptionResourceRequestProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_159 _enclosing;
			}

			private readonly PreemptionContractPBImpl _enclosing;
		}

		private void InitPreemptionContainers()
		{
			if (containers != null)
			{
				return;
			}
			YarnProtos.PreemptionContractProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.PreemptionContainerProto> list = p.GetContainerList();
			containers = new HashSet<PreemptionContainer>();
			foreach (YarnProtos.PreemptionContainerProto c in list)
			{
				containers.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void AddContainersToProto()
		{
			MaybeInitBuilder();
			builder.ClearContainer();
			if (null == containers)
			{
				return;
			}
			IEnumerable<YarnProtos.PreemptionContainerProto> iterable = new _IEnumerable_208(
				this);
			builder.AddAllContainer(iterable);
		}

		private sealed class _IEnumerable_208 : IEnumerable<YarnProtos.PreemptionContainerProto
			>
		{
			public _IEnumerable_208(PreemptionContractPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.PreemptionContainerProto> GetEnumerator()
			{
				return new _IEnumerator_211(this);
			}

			private sealed class _IEnumerator_211 : IEnumerator<YarnProtos.PreemptionContainerProto
				>
			{
				public _IEnumerator_211(_IEnumerable_208 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.containers.GetEnumerator();
				}

				internal IEnumerator<PreemptionContainer> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.PreemptionContainerProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_208 _enclosing;
			}

			private readonly PreemptionContractPBImpl _enclosing;
		}

		private PreemptionContainerPBImpl ConvertFromProtoFormat(YarnProtos.PreemptionContainerProto
			 p)
		{
			return new PreemptionContainerPBImpl(p);
		}

		private YarnProtos.PreemptionContainerProto ConvertToProtoFormat(PreemptionContainer
			 t)
		{
			return ((PreemptionContainerPBImpl)t).GetProto();
		}

		private PreemptionResourceRequestPBImpl ConvertFromProtoFormat(YarnProtos.PreemptionResourceRequestProto
			 p)
		{
			return new PreemptionResourceRequestPBImpl(p);
		}

		private YarnProtos.PreemptionResourceRequestProto ConvertToProtoFormat(PreemptionResourceRequest
			 t)
		{
			return ((PreemptionResourceRequestPBImpl)t).GetProto();
		}
	}
}
