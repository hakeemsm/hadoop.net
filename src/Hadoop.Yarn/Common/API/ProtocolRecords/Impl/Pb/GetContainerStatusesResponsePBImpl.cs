using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetContainerStatusesResponsePBImpl : GetContainerStatusesResponse
	{
		internal YarnServiceProtos.GetContainerStatusesResponseProto proto = YarnServiceProtos.GetContainerStatusesResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetContainerStatusesResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private IList<ContainerStatus> containerStatuses = null;

		private IDictionary<ContainerId, SerializedException> failedRequests = null;

		public GetContainerStatusesResponsePBImpl()
		{
			builder = YarnServiceProtos.GetContainerStatusesResponseProto.NewBuilder();
		}

		public GetContainerStatusesResponsePBImpl(YarnServiceProtos.GetContainerStatusesResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetContainerStatusesResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetContainerStatusesResponseProto)
				builder.Build());
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
			if (this.containerStatuses != null)
			{
				AddLocalContainerStatusesToProto();
			}
			if (this.failedRequests != null)
			{
				AddFailedRequestsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetContainerStatusesResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetContainerStatusesResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void AddLocalContainerStatusesToProto()
		{
			MaybeInitBuilder();
			builder.ClearStatus();
			if (this.containerStatuses == null)
			{
				return;
			}
			IList<YarnProtos.ContainerStatusProto> protoList = new AList<YarnProtos.ContainerStatusProto
				>();
			foreach (ContainerStatus status in containerStatuses)
			{
				protoList.AddItem(ConvertToProtoFormat(status));
			}
			builder.AddAllStatus(protoList);
		}

		private void AddFailedRequestsToProto()
		{
			MaybeInitBuilder();
			builder.ClearFailedRequests();
			if (this.failedRequests == null)
			{
				return;
			}
			IList<YarnServiceProtos.ContainerExceptionMapProto> protoList = new AList<YarnServiceProtos.ContainerExceptionMapProto
				>();
			foreach (KeyValuePair<ContainerId, SerializedException> entry in this.failedRequests)
			{
				protoList.AddItem(((YarnServiceProtos.ContainerExceptionMapProto)YarnServiceProtos.ContainerExceptionMapProto
					.NewBuilder().SetContainerId(ConvertToProtoFormat(entry.Key)).SetException(ConvertToProtoFormat
					(entry.Value)).Build()));
			}
			builder.AddAllFailedRequests(protoList);
		}

		private void InitLocalContainerStatuses()
		{
			if (this.containerStatuses != null)
			{
				return;
			}
			YarnServiceProtos.GetContainerStatusesResponseProtoOrBuilder p = viaProto ? proto
				 : builder;
			IList<YarnProtos.ContainerStatusProto> statuses = p.GetStatusList();
			this.containerStatuses = new AList<ContainerStatus>();
			foreach (YarnProtos.ContainerStatusProto status in statuses)
			{
				this.containerStatuses.AddItem(ConvertFromProtoFormat(status));
			}
		}

		private void InitFailedRequests()
		{
			if (this.failedRequests != null)
			{
				return;
			}
			YarnServiceProtos.GetContainerStatusesResponseProtoOrBuilder p = viaProto ? proto
				 : builder;
			IList<YarnServiceProtos.ContainerExceptionMapProto> protoList = p.GetFailedRequestsList
				();
			this.failedRequests = new Dictionary<ContainerId, SerializedException>();
			foreach (YarnServiceProtos.ContainerExceptionMapProto ce in protoList)
			{
				this.failedRequests[ConvertFromProtoFormat(ce.GetContainerId())] = ConvertFromProtoFormat
					(ce.GetException());
			}
		}

		public override IList<ContainerStatus> GetContainerStatuses()
		{
			InitLocalContainerStatuses();
			return this.containerStatuses;
		}

		public override void SetContainerStatuses(IList<ContainerStatus> statuses)
		{
			MaybeInitBuilder();
			if (statuses == null)
			{
				builder.ClearStatus();
			}
			this.containerStatuses = statuses;
		}

		public override IDictionary<ContainerId, SerializedException> GetFailedRequests()
		{
			InitFailedRequests();
			return this.failedRequests;
		}

		public override void SetFailedRequests(IDictionary<ContainerId, SerializedException
			> failedRequests)
		{
			MaybeInitBuilder();
			if (failedRequests == null)
			{
				builder.ClearFailedRequests();
			}
			this.failedRequests = failedRequests;
		}

		private ContainerStatusPBImpl ConvertFromProtoFormat(YarnProtos.ContainerStatusProto
			 p)
		{
			return new ContainerStatusPBImpl(p);
		}

		private YarnProtos.ContainerStatusProto ConvertToProtoFormat(ContainerStatus t)
		{
			return ((ContainerStatusPBImpl)t).GetProto();
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}

		private SerializedExceptionPBImpl ConvertFromProtoFormat(YarnProtos.SerializedExceptionProto
			 p)
		{
			return new SerializedExceptionPBImpl(p);
		}

		private YarnProtos.SerializedExceptionProto ConvertToProtoFormat(SerializedException
			 t)
		{
			return ((SerializedExceptionPBImpl)t).GetProto();
		}
	}
}
