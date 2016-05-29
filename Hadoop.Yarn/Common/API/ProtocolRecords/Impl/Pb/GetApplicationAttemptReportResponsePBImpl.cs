using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetApplicationAttemptReportResponsePBImpl : GetApplicationAttemptReportResponse
	{
		internal YarnServiceProtos.GetApplicationAttemptReportResponseProto proto = YarnServiceProtos.GetApplicationAttemptReportResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetApplicationAttemptReportResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private ApplicationAttemptReport applicationAttemptReport = null;

		public GetApplicationAttemptReportResponsePBImpl()
		{
			builder = YarnServiceProtos.GetApplicationAttemptReportResponseProto.NewBuilder();
		}

		public GetApplicationAttemptReportResponsePBImpl(YarnServiceProtos.GetApplicationAttemptReportResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetApplicationAttemptReportResponseProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetApplicationAttemptReportResponseProto
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
			if (this.applicationAttemptReport != null)
			{
				builder.SetApplicationAttemptReport(ConvertToProtoFormat(this.applicationAttemptReport
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
			proto = ((YarnServiceProtos.GetApplicationAttemptReportResponseProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetApplicationAttemptReportResponseProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		public override ApplicationAttemptReport GetApplicationAttemptReport()
		{
			if (this.applicationAttemptReport != null)
			{
				return this.applicationAttemptReport;
			}
			YarnServiceProtos.GetApplicationAttemptReportResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasApplicationAttemptReport())
			{
				return null;
			}
			this.applicationAttemptReport = ConvertFromProtoFormat(p.GetApplicationAttemptReport
				());
			return this.applicationAttemptReport;
		}

		public override void SetApplicationAttemptReport(ApplicationAttemptReport ApplicationAttemptReport
			)
		{
			MaybeInitBuilder();
			if (ApplicationAttemptReport == null)
			{
				builder.ClearApplicationAttemptReport();
			}
			this.applicationAttemptReport = ApplicationAttemptReport;
		}

		private ApplicationAttemptReportPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationAttemptReportProto
			 p)
		{
			return new ApplicationAttemptReportPBImpl(p);
		}

		private YarnProtos.ApplicationAttemptReportProto ConvertToProtoFormat(ApplicationAttemptReport
			 t)
		{
			return ((ApplicationAttemptReportPBImpl)t).GetProto();
		}
	}
}
