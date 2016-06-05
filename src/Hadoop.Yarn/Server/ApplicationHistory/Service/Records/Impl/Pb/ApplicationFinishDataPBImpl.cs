using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.Impl.PB
{
	public class ApplicationFinishDataPBImpl : ApplicationFinishData
	{
		internal ApplicationHistoryServerProtos.ApplicationFinishDataProto proto = ApplicationHistoryServerProtos.ApplicationFinishDataProto
			.GetDefaultInstance();

		internal ApplicationHistoryServerProtos.ApplicationFinishDataProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private ApplicationId applicationId;

		public ApplicationFinishDataPBImpl()
		{
			builder = ApplicationHistoryServerProtos.ApplicationFinishDataProto.NewBuilder();
		}

		public ApplicationFinishDataPBImpl(ApplicationHistoryServerProtos.ApplicationFinishDataProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override ApplicationId GetApplicationId()
		{
			if (this.applicationId != null)
			{
				return this.applicationId;
			}
			ApplicationHistoryServerProtos.ApplicationFinishDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasApplicationId())
			{
				return null;
			}
			this.applicationId = ConvertFromProtoFormat(p.GetApplicationId());
			return this.applicationId;
		}

		public override void SetApplicationId(ApplicationId applicationId)
		{
			MaybeInitBuilder();
			if (applicationId == null)
			{
				builder.ClearApplicationId();
			}
			this.applicationId = applicationId;
		}

		public override long GetFinishTime()
		{
			ApplicationHistoryServerProtos.ApplicationFinishDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			return p.GetFinishTime();
		}

		public override void SetFinishTime(long finishTime)
		{
			MaybeInitBuilder();
			builder.SetFinishTime(finishTime);
		}

		public override string GetDiagnosticsInfo()
		{
			ApplicationHistoryServerProtos.ApplicationFinishDataProtoOrBuilder p = viaProto ? 
				proto : builder;
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

		public override FinalApplicationStatus GetFinalApplicationStatus()
		{
			ApplicationHistoryServerProtos.ApplicationFinishDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasFinalApplicationStatus())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetFinalApplicationStatus());
		}

		public override void SetFinalApplicationStatus(FinalApplicationStatus finalApplicationStatus
			)
		{
			MaybeInitBuilder();
			if (finalApplicationStatus == null)
			{
				builder.ClearFinalApplicationStatus();
				return;
			}
			builder.SetFinalApplicationStatus(ConvertToProtoFormat(finalApplicationStatus));
		}

		public override YarnApplicationState GetYarnApplicationState()
		{
			ApplicationHistoryServerProtos.ApplicationFinishDataProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasYarnApplicationState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetYarnApplicationState());
		}

		public override void SetYarnApplicationState(YarnApplicationState state)
		{
			MaybeInitBuilder();
			if (state == null)
			{
				builder.ClearYarnApplicationState();
				return;
			}
			builder.SetYarnApplicationState(ConvertToProtoFormat(state));
		}

		public virtual ApplicationHistoryServerProtos.ApplicationFinishDataProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((ApplicationHistoryServerProtos.ApplicationFinishDataProto
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
			if (this.applicationId != null && !((ApplicationIdPBImpl)this.applicationId).GetProto
				().Equals(builder.GetApplicationId()))
			{
				builder.SetApplicationId(ConvertToProtoFormat(this.applicationId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((ApplicationHistoryServerProtos.ApplicationFinishDataProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = ApplicationHistoryServerProtos.ApplicationFinishDataProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		private YarnProtos.ApplicationIdProto ConvertToProtoFormat(ApplicationId applicationId
			)
		{
			return ((ApplicationIdPBImpl)applicationId).GetProto();
		}

		private ApplicationIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationIdProto 
			applicationId)
		{
			return new ApplicationIdPBImpl(applicationId);
		}

		private FinalApplicationStatus ConvertFromProtoFormat(YarnProtos.FinalApplicationStatusProto
			 finalApplicationStatus)
		{
			return ProtoUtils.ConvertFromProtoFormat(finalApplicationStatus);
		}

		private YarnProtos.FinalApplicationStatusProto ConvertToProtoFormat(FinalApplicationStatus
			 finalApplicationStatus)
		{
			return ProtoUtils.ConvertToProtoFormat(finalApplicationStatus);
		}

		private YarnProtos.YarnApplicationStateProto ConvertToProtoFormat(YarnApplicationState
			 state)
		{
			return ProtoUtils.ConvertToProtoFormat(state);
		}

		private YarnApplicationState ConvertFromProtoFormat(YarnProtos.YarnApplicationStateProto
			 yarnApplicationState)
		{
			return ProtoUtils.ConvertFromProtoFormat(yarnApplicationState);
		}
	}
}
