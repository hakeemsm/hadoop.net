using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.Impl.PB
{
	public class ApplicationAttemptFinishDataPBImpl : ApplicationAttemptFinishData
	{
		internal ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto proto = 
			ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto.GetDefaultInstance
			();

		internal ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto.Builder
			 builder = null;

		internal bool viaProto = false;

		public ApplicationAttemptFinishDataPBImpl()
		{
			builder = ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto.NewBuilder
				();
		}

		public ApplicationAttemptFinishDataPBImpl(ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		private ApplicationAttemptId applicationAttemptId;

		public override ApplicationAttemptId GetApplicationAttemptId()
		{
			if (this.applicationAttemptId != null)
			{
				return this.applicationAttemptId;
			}
			ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProtoOrBuilder p = viaProto
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

		public override string GetTrackingURL()
		{
			ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasTrackingUrl())
			{
				return null;
			}
			return p.GetTrackingUrl();
		}

		public override void SetTrackingURL(string trackingURL)
		{
			MaybeInitBuilder();
			if (trackingURL == null)
			{
				builder.ClearTrackingUrl();
				return;
			}
			builder.SetTrackingUrl(trackingURL);
		}

		public override string GetDiagnosticsInfo()
		{
			ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProtoOrBuilder p = viaProto
				 ? proto : builder;
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
			ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProtoOrBuilder p = viaProto
				 ? proto : builder;
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

		public override YarnApplicationAttemptState GetYarnApplicationAttemptState()
		{
			ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasYarnApplicationAttemptState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetYarnApplicationAttemptState());
		}

		public override void SetYarnApplicationAttemptState(YarnApplicationAttemptState state
			)
		{
			MaybeInitBuilder();
			if (state == null)
			{
				builder.ClearYarnApplicationAttemptState();
				return;
			}
			builder.SetYarnApplicationAttemptState(ConvertToProtoFormat(state));
		}

		public virtual ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto
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
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto)builder
				.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto.NewBuilder
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

		private YarnProtos.YarnApplicationAttemptStateProto ConvertToProtoFormat(YarnApplicationAttemptState
			 state)
		{
			return ProtoUtils.ConvertToProtoFormat(state);
		}

		private YarnApplicationAttemptState ConvertFromProtoFormat(YarnProtos.YarnApplicationAttemptStateProto
			 yarnApplicationAttemptState)
		{
			return ProtoUtils.ConvertFromProtoFormat(yarnApplicationAttemptState);
		}
	}
}
