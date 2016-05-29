using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ApplicationReportPBImpl : ApplicationReport
	{
		internal YarnProtos.ApplicationReportProto proto = YarnProtos.ApplicationReportProto
			.GetDefaultInstance();

		internal YarnProtos.ApplicationReportProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationId applicationId;

		private ApplicationAttemptId currentApplicationAttemptId;

		private Token clientToAMToken = null;

		private Token amRmToken = null;

		private ICollection<string> applicationTags = null;

		public ApplicationReportPBImpl()
		{
			builder = YarnProtos.ApplicationReportProto.NewBuilder();
		}

		public ApplicationReportPBImpl(YarnProtos.ApplicationReportProto proto)
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
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasApplicationId())
			{
				return null;
			}
			this.applicationId = ConvertFromProtoFormat(p.GetApplicationId());
			return this.applicationId;
		}

		public override void SetApplicationResourceUsageReport(ApplicationResourceUsageReport
			 appInfo)
		{
			MaybeInitBuilder();
			if (appInfo == null)
			{
				builder.ClearAppResourceUsage();
				return;
			}
			builder.SetAppResourceUsage(ConvertToProtoFormat(appInfo));
		}

		public override ApplicationAttemptId GetCurrentApplicationAttemptId()
		{
			if (this.currentApplicationAttemptId != null)
			{
				return this.currentApplicationAttemptId;
			}
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasCurrentApplicationAttemptId())
			{
				return null;
			}
			this.currentApplicationAttemptId = ConvertFromProtoFormat(p.GetCurrentApplicationAttemptId
				());
			return this.currentApplicationAttemptId;
		}

		public override ApplicationResourceUsageReport GetApplicationResourceUsageReport(
			)
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasAppResourceUsage())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetAppResourceUsage());
		}

		public override string GetTrackingUrl()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasTrackingUrl())
			{
				return null;
			}
			return p.GetTrackingUrl();
		}

		public override string GetOriginalTrackingUrl()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasOriginalTrackingUrl())
			{
				return null;
			}
			return p.GetOriginalTrackingUrl();
		}

		public override string GetName()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasName())
			{
				return null;
			}
			return p.GetName();
		}

		public override string GetQueue()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasQueue())
			{
				return null;
			}
			return p.GetQueue();
		}

		public override YarnApplicationState GetYarnApplicationState()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasYarnApplicationState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetYarnApplicationState());
		}

		public override string GetHost()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasHost())
			{
				return null;
			}
			return (p.GetHost());
		}

		public override int GetRpcPort()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetRpcPort());
		}

		public override Token GetClientToAMToken()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (this.clientToAMToken != null)
			{
				return this.clientToAMToken;
			}
			if (!p.HasClientToAmToken())
			{
				return null;
			}
			this.clientToAMToken = ConvertFromProtoFormat(p.GetClientToAmToken());
			return this.clientToAMToken;
		}

		public override string GetUser()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasUser())
			{
				return null;
			}
			return p.GetUser();
		}

		public override string GetDiagnostics()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasDiagnostics())
			{
				return null;
			}
			return p.GetDiagnostics();
		}

		public override long GetStartTime()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetStartTime();
		}

		public override long GetFinishTime()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetFinishTime();
		}

		public override FinalApplicationStatus GetFinalApplicationStatus()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasFinalApplicationStatus())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetFinalApplicationStatus());
		}

		public override float GetProgress()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetProgress();
		}

		public override string GetApplicationType()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasApplicationType())
			{
				return null;
			}
			return p.GetApplicationType();
		}

		public override Token GetAMRMToken()
		{
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			if (amRmToken != null)
			{
				return amRmToken;
			}
			if (!p.HasAmRmToken())
			{
				return null;
			}
			amRmToken = ConvertFromProtoFormat(p.GetAmRmToken());
			return amRmToken;
		}

		private void InitApplicationTags()
		{
			if (this.applicationTags != null)
			{
				return;
			}
			YarnProtos.ApplicationReportProtoOrBuilder p = viaProto ? proto : builder;
			this.applicationTags = new HashSet<string>();
			Sharpen.Collections.AddAll(this.applicationTags, p.GetApplicationTagsList());
		}

		public override ICollection<string> GetApplicationTags()
		{
			InitApplicationTags();
			return this.applicationTags;
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

		public override void SetCurrentApplicationAttemptId(ApplicationAttemptId applicationAttemptId
			)
		{
			MaybeInitBuilder();
			if (applicationAttemptId == null)
			{
				builder.ClearCurrentApplicationAttemptId();
			}
			this.currentApplicationAttemptId = applicationAttemptId;
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

		public override void SetOriginalTrackingUrl(string url)
		{
			MaybeInitBuilder();
			if (url == null)
			{
				builder.ClearOriginalTrackingUrl();
				return;
			}
			builder.SetOriginalTrackingUrl(url);
		}

		public override void SetName(string name)
		{
			MaybeInitBuilder();
			if (name == null)
			{
				builder.ClearName();
				return;
			}
			builder.SetName(name);
		}

		public override void SetQueue(string queue)
		{
			MaybeInitBuilder();
			if (queue == null)
			{
				builder.ClearQueue();
				return;
			}
			builder.SetQueue(queue);
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

		public override void SetHost(string host)
		{
			MaybeInitBuilder();
			if (host == null)
			{
				builder.ClearHost();
				return;
			}
			builder.SetHost((host));
		}

		public override void SetRpcPort(int rpcPort)
		{
			MaybeInitBuilder();
			builder.SetRpcPort((rpcPort));
		}

		public override void SetClientToAMToken(Token clientToAMToken)
		{
			MaybeInitBuilder();
			if (clientToAMToken == null)
			{
				builder.ClearClientToAmToken();
			}
			this.clientToAMToken = clientToAMToken;
		}

		public override void SetUser(string user)
		{
			MaybeInitBuilder();
			if (user == null)
			{
				builder.ClearUser();
				return;
			}
			builder.SetUser((user));
		}

		public override void SetApplicationType(string applicationType)
		{
			MaybeInitBuilder();
			if (applicationType == null)
			{
				builder.ClearApplicationType();
				return;
			}
			builder.SetApplicationType((applicationType));
		}

		public override void SetApplicationTags(ICollection<string> tags)
		{
			MaybeInitBuilder();
			if (tags == null || tags.IsEmpty())
			{
				builder.ClearApplicationTags();
			}
			this.applicationTags = tags;
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

		public override void SetStartTime(long startTime)
		{
			MaybeInitBuilder();
			builder.SetStartTime(startTime);
		}

		public override void SetFinishTime(long finishTime)
		{
			MaybeInitBuilder();
			builder.SetFinishTime(finishTime);
		}

		public override void SetFinalApplicationStatus(FinalApplicationStatus finishState
			)
		{
			MaybeInitBuilder();
			if (finishState == null)
			{
				builder.ClearFinalApplicationStatus();
				return;
			}
			builder.SetFinalApplicationStatus(ConvertToProtoFormat(finishState));
		}

		public override void SetProgress(float progress)
		{
			MaybeInitBuilder();
			builder.SetProgress(progress);
		}

		public override void SetAMRMToken(Token amRmToken)
		{
			MaybeInitBuilder();
			if (amRmToken == null)
			{
				builder.ClearAmRmToken();
			}
			this.amRmToken = amRmToken;
		}

		public virtual YarnProtos.ApplicationReportProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ApplicationReportProto)builder.Build());
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
			if (this.currentApplicationAttemptId != null && !((ApplicationAttemptIdPBImpl)this
				.currentApplicationAttemptId).GetProto().Equals(builder.GetCurrentApplicationAttemptId
				()))
			{
				builder.SetCurrentApplicationAttemptId(ConvertToProtoFormat(this.currentApplicationAttemptId
					));
			}
			if (this.clientToAMToken != null && !((TokenPBImpl)this.clientToAMToken).GetProto
				().Equals(builder.GetClientToAmToken()))
			{
				builder.SetClientToAmToken(ConvertToProtoFormat(this.clientToAMToken));
			}
			if (this.amRmToken != null && !((TokenPBImpl)this.amRmToken).GetProto().Equals(builder
				.GetAmRmToken()))
			{
				builder.SetAmRmToken(ConvertToProtoFormat(this.amRmToken));
			}
			if (this.applicationTags != null && !this.applicationTags.IsEmpty())
			{
				builder.ClearApplicationTags();
				builder.AddAllApplicationTags(this.applicationTags);
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ApplicationReportProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ApplicationReportProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private YarnProtos.ApplicationIdProto ConvertToProtoFormat(ApplicationId t)
		{
			return ((ApplicationIdPBImpl)t).GetProto();
		}

		private YarnProtos.ApplicationAttemptIdProto ConvertToProtoFormat(ApplicationAttemptId
			 t)
		{
			return ((ApplicationAttemptIdPBImpl)t).GetProto();
		}

		private ApplicationResourceUsageReport ConvertFromProtoFormat(YarnProtos.ApplicationResourceUsageReportProto
			 s)
		{
			return ProtoUtils.ConvertFromProtoFormat(s);
		}

		private YarnProtos.ApplicationResourceUsageReportProto ConvertToProtoFormat(ApplicationResourceUsageReport
			 s)
		{
			return ProtoUtils.ConvertToProtoFormat(s);
		}

		private ApplicationIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationIdProto 
			applicationId)
		{
			return new ApplicationIdPBImpl(applicationId);
		}

		private ApplicationAttemptIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationAttemptIdProto
			 applicationAttemptId)
		{
			return new ApplicationAttemptIdPBImpl(applicationAttemptId);
		}

		private YarnApplicationState ConvertFromProtoFormat(YarnProtos.YarnApplicationStateProto
			 s)
		{
			return ProtoUtils.ConvertFromProtoFormat(s);
		}

		private YarnProtos.YarnApplicationStateProto ConvertToProtoFormat(YarnApplicationState
			 s)
		{
			return ProtoUtils.ConvertToProtoFormat(s);
		}

		private FinalApplicationStatus ConvertFromProtoFormat(YarnProtos.FinalApplicationStatusProto
			 s)
		{
			return ProtoUtils.ConvertFromProtoFormat(s);
		}

		private YarnProtos.FinalApplicationStatusProto ConvertToProtoFormat(FinalApplicationStatus
			 s)
		{
			return ProtoUtils.ConvertToProtoFormat(s);
		}

		private TokenPBImpl ConvertFromProtoFormat(SecurityProtos.TokenProto p)
		{
			return new TokenPBImpl(p);
		}

		private SecurityProtos.TokenProto ConvertToProtoFormat(Token t)
		{
			return ((TokenPBImpl)t).GetProto();
		}
	}
}
