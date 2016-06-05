using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ApplicationAttemptReportPBImpl : ApplicationAttemptReport
	{
		internal YarnProtos.ApplicationAttemptReportProto proto = YarnProtos.ApplicationAttemptReportProto
			.GetDefaultInstance();

		internal YarnProtos.ApplicationAttemptReportProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationAttemptId ApplicationAttemptId;

		private ContainerId amContainerId;

		public ApplicationAttemptReportPBImpl()
		{
			builder = YarnProtos.ApplicationAttemptReportProto.NewBuilder();
		}

		public ApplicationAttemptReportPBImpl(YarnProtos.ApplicationAttemptReportProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override ApplicationAttemptId GetApplicationAttemptId()
		{
			if (this.ApplicationAttemptId != null)
			{
				return this.ApplicationAttemptId;
			}
			YarnProtos.ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasApplicationAttemptId())
			{
				return null;
			}
			this.ApplicationAttemptId = ConvertFromProtoFormat(p.GetApplicationAttemptId());
			return this.ApplicationAttemptId;
		}

		public override string GetHost()
		{
			YarnProtos.ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasHost())
			{
				return null;
			}
			return p.GetHost();
		}

		public override int GetRpcPort()
		{
			YarnProtos.ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetRpcPort();
		}

		public override string GetTrackingUrl()
		{
			YarnProtos.ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasTrackingUrl())
			{
				return null;
			}
			return p.GetTrackingUrl();
		}

		public override string GetOriginalTrackingUrl()
		{
			YarnProtos.ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasOriginalTrackingUrl())
			{
				return null;
			}
			return p.GetOriginalTrackingUrl();
		}

		public override string GetDiagnostics()
		{
			YarnProtos.ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasDiagnostics())
			{
				return null;
			}
			return p.GetDiagnostics();
		}

		public override YarnApplicationAttemptState GetYarnApplicationAttemptState()
		{
			YarnProtos.ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
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

		public override void SetApplicationAttemptId(ApplicationAttemptId applicationAttemptId
			)
		{
			MaybeInitBuilder();
			if (applicationAttemptId == null)
			{
				builder.ClearApplicationAttemptId();
			}
			this.ApplicationAttemptId = applicationAttemptId;
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

		public override void SetRpcPort(int rpcPort)
		{
			MaybeInitBuilder();
			builder.SetRpcPort(rpcPort);
		}

		public override void SetTrackingUrl(string url)
		{
			MaybeInitBuilder();
			if (url == null)
			{
				builder.ClearTrackingUrl();
				return;
			}
			builder.SetTrackingUrl(url);
		}

		public override void SetOriginalTrackingUrl(string oUrl)
		{
			MaybeInitBuilder();
			if (oUrl == null)
			{
				builder.ClearOriginalTrackingUrl();
				return;
			}
			builder.SetOriginalTrackingUrl(oUrl);
		}

		public override void SetDiagnostics(string diagnostics)
		{
			MaybeInitBuilder();
			if (diagnostics == null)
			{
				builder.ClearDiagnostics();
				return;
			}
			builder.SetDiagnostics(diagnostics);
		}

		public virtual YarnProtos.ApplicationAttemptReportProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ApplicationAttemptReportProto)builder.Build
				());
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

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ApplicationAttemptReportProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ApplicationAttemptReportProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (this.ApplicationAttemptId != null && !((ApplicationAttemptIdPBImpl)this.ApplicationAttemptId
				).GetProto().Equals(builder.GetApplicationAttemptId()))
			{
				builder.SetApplicationAttemptId(ConvertToProtoFormat(this.ApplicationAttemptId));
			}
			if (this.amContainerId != null && !((ContainerIdPBImpl)this.amContainerId).GetProto
				().Equals(builder.GetAmContainerId()))
			{
				builder.SetAmContainerId(ConvertToProtoFormat(this.amContainerId));
			}
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId amContainerId
			)
		{
			return ((ContainerIdPBImpl)amContainerId).GetProto();
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto amContainerId
			)
		{
			return new ContainerIdPBImpl(amContainerId);
		}

		private YarnProtos.ApplicationAttemptIdProto ConvertToProtoFormat(ApplicationAttemptId
			 t)
		{
			return ((ApplicationAttemptIdPBImpl)t).GetProto();
		}

		private ApplicationAttemptIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationAttemptIdProto
			 applicationAttemptId)
		{
			return new ApplicationAttemptIdPBImpl(applicationAttemptId);
		}

		public override ContainerId GetAMContainerId()
		{
			if (this.amContainerId != null)
			{
				return this.amContainerId;
			}
			YarnProtos.ApplicationAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasAmContainerId())
			{
				return null;
			}
			this.amContainerId = ConvertFromProtoFormat(p.GetAmContainerId());
			return this.amContainerId;
		}

		public override void SetAMContainerId(ContainerId amContainerId)
		{
			MaybeInitBuilder();
			if (amContainerId == null)
			{
				builder.ClearAmContainerId();
			}
			this.amContainerId = amContainerId;
		}
	}
}
