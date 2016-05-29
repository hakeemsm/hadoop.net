using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records.Impl.PB
{
	public class ApplicationAttemptStateDataPBImpl : ApplicationAttemptStateData
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records.Impl.PB.ApplicationAttemptStateDataPBImpl
			));

		internal YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
			 proto = YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
			.GetDefaultInstance();

		internal YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto.Builder
			 builder = null;

		internal bool viaProto = false;

		private ApplicationAttemptId attemptId = null;

		private Container masterContainer = null;

		private ByteBuffer appAttemptTokens = null;

		public ApplicationAttemptStateDataPBImpl()
		{
			builder = YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
				.NewBuilder();
		}

		public ApplicationAttemptStateDataPBImpl(YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
			 GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.attemptId != null)
			{
				builder.SetAttemptId(((ApplicationAttemptIdPBImpl)attemptId).GetProto());
			}
			if (this.masterContainer != null)
			{
				builder.SetMasterContainer(((ContainerPBImpl)masterContainer).GetProto());
			}
			if (this.appAttemptTokens != null)
			{
				builder.SetAppAttemptTokens(ProtoUtils.ConvertToProtoFormat(this.appAttemptTokens
					));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
				)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto
					.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ApplicationAttemptId GetAttemptId()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			if (attemptId != null)
			{
				return attemptId;
			}
			if (!p.HasAttemptId())
			{
				return null;
			}
			attemptId = new ApplicationAttemptIdPBImpl(p.GetAttemptId());
			return attemptId;
		}

		public override void SetAttemptId(ApplicationAttemptId attemptId)
		{
			MaybeInitBuilder();
			if (attemptId == null)
			{
				builder.ClearAttemptId();
			}
			this.attemptId = attemptId;
		}

		public override Container GetMasterContainer()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			if (masterContainer != null)
			{
				return masterContainer;
			}
			if (!p.HasMasterContainer())
			{
				return null;
			}
			masterContainer = new ContainerPBImpl(p.GetMasterContainer());
			return masterContainer;
		}

		public override void SetMasterContainer(Container container)
		{
			MaybeInitBuilder();
			if (container == null)
			{
				builder.ClearMasterContainer();
			}
			this.masterContainer = container;
		}

		public override Credentials GetAppAttemptTokens()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			if (appAttemptTokens != null)
			{
				return ConvertCredentialsFromByteBuffer(appAttemptTokens);
			}
			if (!p.HasAppAttemptTokens())
			{
				return null;
			}
			this.appAttemptTokens = ProtoUtils.ConvertFromProtoFormat(p.GetAppAttemptTokens()
				);
			return ConvertCredentialsFromByteBuffer(appAttemptTokens);
		}

		public override void SetAppAttemptTokens(Credentials attemptTokens)
		{
			MaybeInitBuilder();
			if (attemptTokens == null)
			{
				builder.ClearAppAttemptTokens();
				return;
			}
			this.appAttemptTokens = ConvertCredentialsToByteBuffer(attemptTokens);
		}

		public override RMAppAttemptState GetState()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			if (!p.HasAppAttemptState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetAppAttemptState());
		}

		public override void SetState(RMAppAttemptState state)
		{
			MaybeInitBuilder();
			if (state == null)
			{
				builder.ClearAppAttemptState();
				return;
			}
			builder.SetAppAttemptState(ConvertToProtoFormat(state));
		}

		public override string GetFinalTrackingUrl()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			if (!p.HasFinalTrackingUrl())
			{
				return null;
			}
			return p.GetFinalTrackingUrl();
		}

		public override void SetFinalTrackingUrl(string url)
		{
			MaybeInitBuilder();
			if (url == null)
			{
				builder.ClearFinalTrackingUrl();
				return;
			}
			builder.SetFinalTrackingUrl(url);
		}

		public override string GetDiagnostics()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
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

		public override long GetStartTime()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			return p.GetStartTime();
		}

		public override void SetStartTime(long startTime)
		{
			MaybeInitBuilder();
			builder.SetStartTime(startTime);
		}

		public override long GetMemorySeconds()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			return p.GetMemorySeconds();
		}

		public override long GetVcoreSeconds()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			return p.GetVcoreSeconds();
		}

		public override void SetMemorySeconds(long memorySeconds)
		{
			MaybeInitBuilder();
			builder.SetMemorySeconds(memorySeconds);
		}

		public override void SetVcoreSeconds(long vcoreSeconds)
		{
			MaybeInitBuilder();
			builder.SetVcoreSeconds(vcoreSeconds);
		}

		public override FinalApplicationStatus GetFinalApplicationStatus()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			if (!p.HasFinalApplicationStatus())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetFinalApplicationStatus());
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

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override int GetAMContainerExitStatus()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			return p.GetAmContainerExitStatus();
		}

		public override void SetAMContainerExitStatus(int exitStatus)
		{
			MaybeInitBuilder();
			builder.SetAmContainerExitStatus(exitStatus);
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

		private static string RmAppAttemptPrefix = "RMATTEMPT_";

		public static YarnServerResourceManagerRecoveryProtos.RMAppAttemptStateProto ConvertToProtoFormat
			(RMAppAttemptState e)
		{
			return YarnServerResourceManagerRecoveryProtos.RMAppAttemptStateProto.ValueOf(RmAppAttemptPrefix
				 + e.ToString());
		}

		public static RMAppAttemptState ConvertFromProtoFormat(YarnServerResourceManagerRecoveryProtos.RMAppAttemptStateProto
			 e)
		{
			return RMAppAttemptState.ValueOf(e.ToString().Replace(RmAppAttemptPrefix, string.Empty
				));
		}

		private YarnProtos.FinalApplicationStatusProto ConvertToProtoFormat(FinalApplicationStatus
			 s)
		{
			return ProtoUtils.ConvertToProtoFormat(s);
		}

		private FinalApplicationStatus ConvertFromProtoFormat(YarnProtos.FinalApplicationStatusProto
			 s)
		{
			return ProtoUtils.ConvertFromProtoFormat(s);
		}

		public override long GetFinishTime()
		{
			YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProtoOrBuilder
				 p = viaProto ? proto : builder;
			return p.GetFinishTime();
		}

		public override void SetFinishTime(long finishTime)
		{
			MaybeInitBuilder();
			builder.SetFinishTime(finishTime);
		}

		private static ByteBuffer ConvertCredentialsToByteBuffer(Credentials credentials)
		{
			ByteBuffer appAttemptTokens = null;
			DataOutputBuffer dob = new DataOutputBuffer();
			try
			{
				if (credentials != null)
				{
					credentials.WriteTokenStorageToStream(dob);
					appAttemptTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
				}
				return appAttemptTokens;
			}
			catch (IOException)
			{
				Log.Error("Failed to convert Credentials to ByteBuffer.");
				System.Diagnostics.Debug.Assert(false);
				return null;
			}
			finally
			{
				IOUtils.CloseStream(dob);
			}
		}

		private static Credentials ConvertCredentialsFromByteBuffer(ByteBuffer appAttemptTokens
			)
		{
			DataInputByteBuffer dibb = new DataInputByteBuffer();
			try
			{
				Credentials credentials = null;
				if (appAttemptTokens != null)
				{
					credentials = new Credentials();
					appAttemptTokens.Rewind();
					dibb.Reset(appAttemptTokens);
					credentials.ReadTokenStorageStream(dibb);
				}
				return credentials;
			}
			catch (IOException)
			{
				Log.Error("Failed to convert Credentials from ByteBuffer.");
				System.Diagnostics.Debug.Assert(false);
				return null;
			}
			finally
			{
				IOUtils.CloseStream(dibb);
			}
		}
	}
}
