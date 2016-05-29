using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.Impl.PB
{
	public class ApplicationAttemptStartDataPBImpl : ApplicationAttemptStartData
	{
		internal ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto proto = 
			ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto.GetDefaultInstance
			();

		internal ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto.Builder 
			builder = null;

		internal bool viaProto = false;

		public ApplicationAttemptStartDataPBImpl()
		{
			builder = ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto.NewBuilder
				();
		}

		public ApplicationAttemptStartDataPBImpl(ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		private ApplicationAttemptId applicationAttemptId;

		private ContainerId masterContainerId;

		public override ApplicationAttemptId GetApplicationAttemptId()
		{
			if (this.applicationAttemptId != null)
			{
				return this.applicationAttemptId;
			}
			ApplicationHistoryServerProtos.ApplicationAttemptStartDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasApplicationAttemptId())
			{
				return null;
			}
			this.applicationAttemptId = ConvertFromProtoFormat(p.GetApplicationAttemptId());
			return this.applicationAttemptId;
		}

		public override void SetApplicationAttemptId(ApplicationAttemptId applicationAttemptId
			)
		{
			MaybeInitBuilder();
			if (applicationAttemptId == null)
			{
				builder.ClearApplicationAttemptId();
			}
			this.applicationAttemptId = applicationAttemptId;
		}

		public override string GetHost()
		{
			ApplicationHistoryServerProtos.ApplicationAttemptStartDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasHost())
			{
				return null;
			}
			return p.GetHost();
		}

		public override void SetHost(string host)
		{
			MaybeInitBuilder();
			if (host == null)
			{
				builder.ClearHost();
				return;
			}
			builder.SetHost(host);
		}

		public override int GetRPCPort()
		{
			ApplicationHistoryServerProtos.ApplicationAttemptStartDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			return p.GetRpcPort();
		}

		public override void SetRPCPort(int rpcPort)
		{
			MaybeInitBuilder();
			builder.SetRpcPort(rpcPort);
		}

		public override ContainerId GetMasterContainerId()
		{
			if (this.masterContainerId != null)
			{
				return this.masterContainerId;
			}
			ApplicationHistoryServerProtos.ApplicationAttemptStartDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasApplicationAttemptId())
			{
				return null;
			}
			this.masterContainerId = ConvertFromProtoFormat(p.GetMasterContainerId());
			return this.masterContainerId;
		}

		public override void SetMasterContainerId(ContainerId masterContainerId)
		{
			MaybeInitBuilder();
			if (masterContainerId == null)
			{
				builder.ClearMasterContainerId();
			}
			this.masterContainerId = masterContainerId;
		}

		public virtual ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto
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
			if (this.applicationAttemptId != null && !((ApplicationAttemptIdPBImpl)this.applicationAttemptId
				).GetProto().Equals(builder.GetApplicationAttemptId()))
			{
				builder.SetApplicationAttemptId(ConvertToProtoFormat(this.applicationAttemptId));
			}
			if (this.masterContainerId != null && !((ContainerIdPBImpl)this.masterContainerId
				).GetProto().Equals(builder.GetMasterContainerId()))
			{
				builder.SetMasterContainerId(ConvertToProtoFormat(this.masterContainerId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto)builder
				.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}

		private ApplicationAttemptIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationAttemptIdProto
			 applicationAttemptId)
		{
			return new ApplicationAttemptIdPBImpl(applicationAttemptId);
		}

		private YarnProtos.ApplicationAttemptIdProto ConvertToProtoFormat(ApplicationAttemptId
			 applicationAttemptId)
		{
			return ((ApplicationAttemptIdPBImpl)applicationAttemptId).GetProto();
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto containerId
			)
		{
			return new ContainerIdPBImpl(containerId);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId masterContainerId
			)
		{
			return ((ContainerIdPBImpl)masterContainerId).GetProto();
		}
	}
}
