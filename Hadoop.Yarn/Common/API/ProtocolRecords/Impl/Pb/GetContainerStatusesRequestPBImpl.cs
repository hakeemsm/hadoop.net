using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetContainerStatusesRequestPBImpl : GetContainerStatusesRequest
	{
		internal YarnServiceProtos.GetContainerStatusesRequestProto proto = YarnServiceProtos.GetContainerStatusesRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetContainerStatusesRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private IList<ContainerId> containerIds = null;

		public GetContainerStatusesRequestPBImpl()
		{
			builder = YarnServiceProtos.GetContainerStatusesRequestProto.NewBuilder();
		}

		public GetContainerStatusesRequestPBImpl(YarnServiceProtos.GetContainerStatusesRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetContainerStatusesRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetContainerStatusesRequestProto)builder
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
			if (this.containerIds != null)
			{
				AddLocalContainerIdsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetContainerStatusesRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetContainerStatusesRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void AddLocalContainerIdsToProto()
		{
			MaybeInitBuilder();
			builder.ClearContainerId();
			if (this.containerIds == null)
			{
				return;
			}
			IList<YarnProtos.ContainerIdProto> protoList = new AList<YarnProtos.ContainerIdProto
				>();
			foreach (ContainerId id in containerIds)
			{
				protoList.AddItem(ConvertToProtoFormat(id));
			}
			builder.AddAllContainerId(protoList);
		}

		private void InitLocalContainerIds()
		{
			if (this.containerIds != null)
			{
				return;
			}
			YarnServiceProtos.GetContainerStatusesRequestProtoOrBuilder p = viaProto ? proto : 
				builder;
			IList<YarnProtos.ContainerIdProto> containerIds = p.GetContainerIdList();
			this.containerIds = new AList<ContainerId>();
			foreach (YarnProtos.ContainerIdProto id in containerIds)
			{
				this.containerIds.AddItem(ConvertFromProtoFormat(id));
			}
		}

		public override IList<ContainerId> GetContainerIds()
		{
			InitLocalContainerIds();
			return this.containerIds;
		}

		public override void SetContainerIds(IList<ContainerId> containerIds)
		{
			MaybeInitBuilder();
			if (containerIds == null)
			{
				builder.ClearContainerId();
			}
			this.containerIds = containerIds;
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
