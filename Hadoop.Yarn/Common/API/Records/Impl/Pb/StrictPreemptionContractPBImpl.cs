using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class StrictPreemptionContractPBImpl : StrictPreemptionContract
	{
		internal YarnProtos.StrictPreemptionContractProto proto = YarnProtos.StrictPreemptionContractProto
			.GetDefaultInstance();

		internal YarnProtos.StrictPreemptionContractProto.Builder builder = null;

		internal bool viaProto = false;

		private ICollection<PreemptionContainer> containers;

		public StrictPreemptionContractPBImpl()
		{
			builder = YarnProtos.StrictPreemptionContractProto.NewBuilder();
		}

		public StrictPreemptionContractPBImpl(YarnProtos.StrictPreemptionContractProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.StrictPreemptionContractProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnProtos.StrictPreemptionContractProto)builder.Build
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

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.StrictPreemptionContractProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (this.containers != null)
			{
				AddContainersToProto();
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.StrictPreemptionContractProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ICollection<PreemptionContainer> GetContainers()
		{
			lock (this)
			{
				InitIds();
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

		private void InitIds()
		{
			if (containers != null)
			{
				return;
			}
			YarnProtos.StrictPreemptionContractProtoOrBuilder p = viaProto ? proto : builder;
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
			if (containers == null)
			{
				return;
			}
			IEnumerable<YarnProtos.PreemptionContainerProto> iterable = new _IEnumerable_137(
				this);
			builder.AddAllContainer(iterable);
		}

		private sealed class _IEnumerable_137 : IEnumerable<YarnProtos.PreemptionContainerProto
			>
		{
			public _IEnumerable_137(StrictPreemptionContractPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.PreemptionContainerProto> GetEnumerator()
			{
				return new _IEnumerator_140(this);
			}

			private sealed class _IEnumerator_140 : IEnumerator<YarnProtos.PreemptionContainerProto
				>
			{
				public _IEnumerator_140(_IEnumerable_137 _enclosing)
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

				private readonly _IEnumerable_137 _enclosing;
			}

			private readonly StrictPreemptionContractPBImpl _enclosing;
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
	}
}
