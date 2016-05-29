using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class KillApplicationRequestPBImpl : KillApplicationRequest
	{
		internal YarnServiceProtos.KillApplicationRequestProto proto = YarnServiceProtos.KillApplicationRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.KillApplicationRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationId applicationId = null;

		public KillApplicationRequestPBImpl()
		{
			builder = YarnServiceProtos.KillApplicationRequestProto.NewBuilder();
		}

		public KillApplicationRequestPBImpl(YarnServiceProtos.KillApplicationRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.KillApplicationRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.KillApplicationRequestProto)builder
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
			if (this.applicationId != null)
			{
				builder.SetApplicationId(ConvertToProtoFormat(this.applicationId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.KillApplicationRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.KillApplicationRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ApplicationId GetApplicationId()
		{
			YarnServiceProtos.KillApplicationRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.applicationId != null)
			{
				return this.applicationId;
			}
			if (!p.HasApplicationId())
			{
				return null;
			}
			this.applicationId = ConvertFromProtoFormat(p.GetApplicationId());
			return this.applicationId;
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

		private ApplicationIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationIdProto 
			p)
		{
			return new ApplicationIdPBImpl(p);
		}

		private YarnProtos.ApplicationIdProto ConvertToProtoFormat(ApplicationId t)
		{
			return ((ApplicationIdPBImpl)t).GetProto();
		}
	}
}
