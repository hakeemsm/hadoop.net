using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class FinishApplicationMasterRequestPBImpl : FinishApplicationMasterRequest
	{
		internal YarnServiceProtos.FinishApplicationMasterRequestProto proto = YarnServiceProtos.FinishApplicationMasterRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.FinishApplicationMasterRequestProto.Builder builder = 
			null;

		internal bool viaProto = false;

		public FinishApplicationMasterRequestPBImpl()
		{
			builder = YarnServiceProtos.FinishApplicationMasterRequestProto.NewBuilder();
		}

		public FinishApplicationMasterRequestPBImpl(YarnServiceProtos.FinishApplicationMasterRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.FinishApplicationMasterRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.FinishApplicationMasterRequestProto
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
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.FinishApplicationMasterRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.FinishApplicationMasterRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override string GetDiagnostics()
		{
			YarnServiceProtos.FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
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

		public override string GetTrackingUrl()
		{
			YarnServiceProtos.FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetTrackingUrl();
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

		public override FinalApplicationStatus GetFinalApplicationStatus()
		{
			YarnServiceProtos.FinishApplicationMasterRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasFinalApplicationStatus())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetFinalApplicationStatus());
		}

		public override void SetFinalApplicationStatus(FinalApplicationStatus finalState)
		{
			MaybeInitBuilder();
			if (finalState == null)
			{
				builder.ClearFinalApplicationStatus();
				return;
			}
			builder.SetFinalApplicationStatus(ConvertToProtoFormat(finalState));
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
	}
}
