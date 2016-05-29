using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records.Impl.PB
{
	public class ApplicationStateDataPBImpl : ApplicationStateData
	{
		internal YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto proto = 
			YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto.GetDefaultInstance
			();

		internal YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto.Builder
			 builder = null;

		internal bool viaProto = false;

		private ApplicationSubmissionContext applicationSubmissionContext = null;

		public ApplicationStateDataPBImpl()
		{
			builder = YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto.NewBuilder
				();
		}

		public ApplicationStateDataPBImpl(YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto
			 GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.applicationSubmissionContext != null)
			{
				builder.SetApplicationSubmissionContext(((ApplicationSubmissionContextPBImpl)applicationSubmissionContext
					).GetProto());
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto)builder
				.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}

		public override long GetSubmitTime()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasSubmitTime())
			{
				return -1;
			}
			return (p.GetSubmitTime());
		}

		public override void SetSubmitTime(long submitTime)
		{
			MaybeInitBuilder();
			builder.SetSubmitTime(submitTime);
		}

		public override long GetStartTime()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			return p.GetStartTime();
		}

		public override void SetStartTime(long startTime)
		{
			MaybeInitBuilder();
			builder.SetStartTime(startTime);
		}

		public override string GetUser()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasUser())
			{
				return null;
			}
			return (p.GetUser());
		}

		public override void SetUser(string user)
		{
			MaybeInitBuilder();
			builder.SetUser(user);
		}

		public override ApplicationSubmissionContext GetApplicationSubmissionContext()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (applicationSubmissionContext != null)
			{
				return applicationSubmissionContext;
			}
			if (!p.HasApplicationSubmissionContext())
			{
				return null;
			}
			applicationSubmissionContext = new ApplicationSubmissionContextPBImpl(p.GetApplicationSubmissionContext
				());
			return applicationSubmissionContext;
		}

		public override void SetApplicationSubmissionContext(ApplicationSubmissionContext
			 context)
		{
			MaybeInitBuilder();
			if (context == null)
			{
				builder.ClearApplicationSubmissionContext();
			}
			this.applicationSubmissionContext = context;
		}

		public override RMAppState GetState()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasApplicationState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetApplicationState());
		}

		public override void SetState(RMAppState finalState)
		{
			MaybeInitBuilder();
			if (finalState == null)
			{
				builder.ClearApplicationState();
				return;
			}
			builder.SetApplicationState(ConvertToProtoFormat(finalState));
		}

		public override string GetDiagnostics()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasDiagnostics())
			{
				return null;
			}
			return p.GetDiagnostics();
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

		public override long GetFinishTime()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder p = viaProto
				 ? proto : builder;
			return p.GetFinishTime();
		}

		public override void SetFinishTime(long finishTime)
		{
			MaybeInitBuilder();
			builder.SetFinishTime(finishTime);
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

		private static string RmAppPrefix = "RMAPP_";

		public static YarnServerResourceManagerRecoveryProtos.RMAppStateProto ConvertToProtoFormat
			(RMAppState e)
		{
			return YarnServerResourceManagerRecoveryProtos.RMAppStateProto.ValueOf(RmAppPrefix
				 + e.ToString());
		}

		public static RMAppState ConvertFromProtoFormat(YarnServerResourceManagerRecoveryProtos.RMAppStateProto
			 e)
		{
			return RMAppState.ValueOf(e.ToString().Replace(RmAppPrefix, string.Empty));
		}
	}
}
