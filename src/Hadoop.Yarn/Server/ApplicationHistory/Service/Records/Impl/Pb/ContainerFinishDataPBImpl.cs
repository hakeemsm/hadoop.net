using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.Impl.PB
{
	public class ContainerFinishDataPBImpl : ContainerFinishData
	{
		internal ApplicationHistoryServerProtos.ContainerFinishDataProto proto = ApplicationHistoryServerProtos.ContainerFinishDataProto
			.GetDefaultInstance();

		internal ApplicationHistoryServerProtos.ContainerFinishDataProto.Builder builder = 
			null;

		internal bool viaProto = false;

		private ContainerId containerId;

		public ContainerFinishDataPBImpl()
		{
			builder = ApplicationHistoryServerProtos.ContainerFinishDataProto.NewBuilder();
		}

		public ContainerFinishDataPBImpl(ApplicationHistoryServerProtos.ContainerFinishDataProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override ContainerId GetContainerId()
		{
			if (this.containerId != null)
			{
				return this.containerId;
			}
			ApplicationHistoryServerProtos.ContainerFinishDataProtoOrBuilder p = viaProto ? proto
				 : builder;
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

		public override long GetFinishTime()
		{
			ApplicationHistoryServerProtos.ContainerFinishDataProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetFinishTime();
		}

		public override void SetFinishTime(long finishTime)
		{
			MaybeInitBuilder();
			builder.SetFinishTime(finishTime);
		}

		public override string GetDiagnosticsInfo()
		{
			ApplicationHistoryServerProtos.ContainerFinishDataProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasDiagnosticsInfo())
			{
				return null;
			}
			return p.GetDiagnosticsInfo();
		}

		public override void SetDiagnosticsInfo(string diagnosticsInfo)
		{
			MaybeInitBuilder();
			if (diagnosticsInfo == null)
			{
				builder.ClearDiagnosticsInfo();
				return;
			}
			builder.SetDiagnosticsInfo(diagnosticsInfo);
		}

		public override int GetContainerExitStatus()
		{
			ApplicationHistoryServerProtos.ContainerFinishDataProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetContainerExitStatus();
		}

		public override ContainerState GetContainerState()
		{
			ApplicationHistoryServerProtos.ContainerFinishDataProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasContainerState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetContainerState());
		}

		public override void SetContainerState(ContainerState state)
		{
			MaybeInitBuilder();
			if (state == null)
			{
				builder.ClearContainerState();
				return;
			}
			builder.SetContainerState(ConvertToProtoFormat(state));
		}

		public override void SetContainerExitStatus(int containerExitStatus)
		{
			MaybeInitBuilder();
			builder.SetContainerExitStatus(containerExitStatus);
		}

		public virtual ApplicationHistoryServerProtos.ContainerFinishDataProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((ApplicationHistoryServerProtos.ContainerFinishDataProto
				)builder.Build());
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
			if (this.containerId != null && !((ContainerIdPBImpl)this.containerId).GetProto()
				.Equals(builder.GetContainerId()))
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
			proto = ((ApplicationHistoryServerProtos.ContainerFinishDataProto)builder.Build()
				);
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = ApplicationHistoryServerProtos.ContainerFinishDataProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId containerId)
		{
			return ((ContainerIdPBImpl)containerId).GetProto();
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto containerId
			)
		{
			return new ContainerIdPBImpl(containerId);
		}

		private YarnProtos.ContainerStateProto ConvertToProtoFormat(ContainerState state)
		{
			return ProtoUtils.ConvertToProtoFormat(state);
		}

		private ContainerState ConvertFromProtoFormat(YarnProtos.ContainerStateProto containerState
			)
		{
			return ProtoUtils.ConvertFromProtoFormat(containerState);
		}
	}
}
