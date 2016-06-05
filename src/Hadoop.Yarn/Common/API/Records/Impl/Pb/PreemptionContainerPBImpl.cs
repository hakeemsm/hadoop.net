using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class PreemptionContainerPBImpl : PreemptionContainer
	{
		internal YarnProtos.PreemptionContainerProto proto = YarnProtos.PreemptionContainerProto
			.GetDefaultInstance();

		internal YarnProtos.PreemptionContainerProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerId id;

		public PreemptionContainerPBImpl()
		{
			builder = YarnProtos.PreemptionContainerProto.NewBuilder();
		}

		public PreemptionContainerPBImpl(YarnProtos.PreemptionContainerProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.PreemptionContainerProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnProtos.PreemptionContainerProto)builder.Build());
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
			proto = ((YarnProtos.PreemptionContainerProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (id != null)
			{
				builder.SetId(ConvertToProtoFormat(id));
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.PreemptionContainerProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ContainerId GetId()
		{
			lock (this)
			{
				YarnProtos.PreemptionContainerProtoOrBuilder p = viaProto ? proto : builder;
				if (id != null)
				{
					return id;
				}
				if (!p.HasId())
				{
					return null;
				}
				id = ConvertFromProtoFormat(p.GetId());
				return id;
			}
		}

		public override void SetId(ContainerId id)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (null == id)
				{
					builder.ClearId();
				}
				this.id = id;
			}
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}
	}
}
