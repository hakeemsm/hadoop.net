using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class SubmitApplicationRequestPBImpl : SubmitApplicationRequest
	{
		internal YarnServiceProtos.SubmitApplicationRequestProto proto = YarnServiceProtos.SubmitApplicationRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.SubmitApplicationRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationSubmissionContext applicationSubmissionContext = null;

		public SubmitApplicationRequestPBImpl()
		{
			builder = YarnServiceProtos.SubmitApplicationRequestProto.NewBuilder();
		}

		public SubmitApplicationRequestPBImpl(YarnServiceProtos.SubmitApplicationRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.SubmitApplicationRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.SubmitApplicationRequestProto)builder
				.Build());
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
			if (this.applicationSubmissionContext != null)
			{
				builder.SetApplicationSubmissionContext(ConvertToProtoFormat(this.applicationSubmissionContext
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
			proto = ((YarnServiceProtos.SubmitApplicationRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.SubmitApplicationRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ApplicationSubmissionContext GetApplicationSubmissionContext()
		{
			YarnServiceProtos.SubmitApplicationRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.applicationSubmissionContext != null)
			{
				return this.applicationSubmissionContext;
			}
			if (!p.HasApplicationSubmissionContext())
			{
				return null;
			}
			this.applicationSubmissionContext = ConvertFromProtoFormat(p.GetApplicationSubmissionContext
				());
			return this.applicationSubmissionContext;
		}

		public override void SetApplicationSubmissionContext(ApplicationSubmissionContext
			 applicationSubmissionContext)
		{
			MaybeInitBuilder();
			if (applicationSubmissionContext == null)
			{
				builder.ClearApplicationSubmissionContext();
			}
			this.applicationSubmissionContext = applicationSubmissionContext;
		}

		private ApplicationSubmissionContextPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationSubmissionContextProto
			 p)
		{
			return new ApplicationSubmissionContextPBImpl(p);
		}

		private YarnProtos.ApplicationSubmissionContextProto ConvertToProtoFormat(ApplicationSubmissionContext
			 t)
		{
			return ((ApplicationSubmissionContextPBImpl)t).GetProto();
		}
	}
}
