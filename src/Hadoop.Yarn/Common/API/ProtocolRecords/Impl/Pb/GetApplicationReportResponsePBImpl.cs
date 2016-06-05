using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetApplicationReportResponsePBImpl : GetApplicationReportResponse
	{
		internal YarnServiceProtos.GetApplicationReportResponseProto proto = YarnServiceProtos.GetApplicationReportResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetApplicationReportResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationReport applicationReport = null;

		public GetApplicationReportResponsePBImpl()
		{
			builder = YarnServiceProtos.GetApplicationReportResponseProto.NewBuilder();
		}

		public GetApplicationReportResponsePBImpl(YarnServiceProtos.GetApplicationReportResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetApplicationReportResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetApplicationReportResponseProto)
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
			if (this.applicationReport != null)
			{
				builder.SetApplicationReport(ConvertToProtoFormat(this.applicationReport));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetApplicationReportResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetApplicationReportResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ApplicationReport GetApplicationReport()
		{
			YarnServiceProtos.GetApplicationReportResponseProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (this.applicationReport != null)
			{
				return this.applicationReport;
			}
			if (!p.HasApplicationReport())
			{
				return null;
			}
			this.applicationReport = ConvertFromProtoFormat(p.GetApplicationReport());
			return this.applicationReport;
		}

		public override void SetApplicationReport(ApplicationReport applicationMaster)
		{
			MaybeInitBuilder();
			if (applicationMaster == null)
			{
				builder.ClearApplicationReport();
			}
			this.applicationReport = applicationMaster;
		}

		private ApplicationReportPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationReportProto
			 p)
		{
			return new ApplicationReportPBImpl(p);
		}

		private YarnProtos.ApplicationReportProto ConvertToProtoFormat(ApplicationReport 
			t)
		{
			return ((ApplicationReportPBImpl)t).GetProto();
		}
	}
}
