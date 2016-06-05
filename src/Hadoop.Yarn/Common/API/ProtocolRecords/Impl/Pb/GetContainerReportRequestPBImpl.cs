using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetContainerReportRequestPBImpl : GetContainerReportRequest
	{
		internal YarnServiceProtos.GetContainerReportRequestProto proto = YarnServiceProtos.GetContainerReportRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetContainerReportRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerId containerId = null;

		public GetContainerReportRequestPBImpl()
		{
			builder = YarnServiceProtos.GetContainerReportRequestProto.NewBuilder();
		}

		public GetContainerReportRequestPBImpl(YarnServiceProtos.GetContainerReportRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetContainerReportRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetContainerReportRequestProto)builder
				.Build());
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
			if (containerId != null)
			{
				builder.SetContainerId(ConvertToProtoFormat(this.containerId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetContainerReportRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetContainerReportRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ContainerId GetContainerId()
		{
			if (this.containerId != null)
			{
				return this.containerId;
			}
			YarnServiceProtos.GetContainerReportRequestProtoOrBuilder p = viaProto ? proto : 
				builder;
			if (!p.HasContainerId())
			{
				return null;
			}
			this.containerId = ConvertFromProtoFormat(p.GetContainerId());
			return this.containerId;
		}

		public override void SetContainerId(ContainerId containerId)
		{
			MaybeInitBuilder();
			if (containerId == null)
			{
				builder.ClearContainerId();
			}
			this.containerId = containerId;
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
