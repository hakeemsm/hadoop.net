using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetContainersRequestPBImpl : GetContainersRequest
	{
		internal YarnServiceProtos.GetContainersRequestProto proto = YarnServiceProtos.GetContainersRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetContainersRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationAttemptId applicationAttemptId = null;

		public GetContainersRequestPBImpl()
		{
			builder = YarnServiceProtos.GetContainersRequestProto.NewBuilder();
		}

		public GetContainersRequestPBImpl(YarnServiceProtos.GetContainersRequestProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetContainersRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetContainersRequestProto)builder.
				Build());
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
			if (applicationAttemptId != null)
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
			proto = ((YarnServiceProtos.GetContainersRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetContainersRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ApplicationAttemptId GetApplicationAttemptId()
		{
			if (this.applicationAttemptId != null)
			{
				return this.applicationAttemptId;
			}
			YarnServiceProtos.GetContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
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

		private ApplicationAttemptIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationAttemptIdProto
			 p)
		{
			return new ApplicationAttemptIdPBImpl(p);
		}

		private YarnProtos.ApplicationAttemptIdProto ConvertToProtoFormat(ApplicationAttemptId
			 t)
		{
			return ((ApplicationAttemptIdPBImpl)t).GetProto();
		}
	}
}
