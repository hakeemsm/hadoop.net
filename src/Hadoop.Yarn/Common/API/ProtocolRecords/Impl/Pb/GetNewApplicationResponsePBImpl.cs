using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetNewApplicationResponsePBImpl : GetNewApplicationResponse
	{
		internal YarnServiceProtos.GetNewApplicationResponseProto proto = YarnServiceProtos.GetNewApplicationResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetNewApplicationResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationId applicationId = null;

		private Resource maximumResourceCapability = null;

		public GetNewApplicationResponsePBImpl()
		{
			builder = YarnServiceProtos.GetNewApplicationResponseProto.NewBuilder();
		}

		public GetNewApplicationResponsePBImpl(YarnServiceProtos.GetNewApplicationResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetNewApplicationResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetNewApplicationResponseProto)builder
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
			if (applicationId != null)
			{
				builder.SetApplicationId(ConvertToProtoFormat(this.applicationId));
			}
			if (maximumResourceCapability != null)
			{
				builder.SetMaximumCapability(ConvertToProtoFormat(this.maximumResourceCapability)
					);
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetNewApplicationResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetNewApplicationResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ApplicationId GetApplicationId()
		{
			if (this.applicationId != null)
			{
				return this.applicationId;
			}
			YarnServiceProtos.GetNewApplicationResponseProtoOrBuilder p = viaProto ? proto : 
				builder;
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

		public override Resource GetMaximumResourceCapability()
		{
			if (this.maximumResourceCapability != null)
			{
				return this.maximumResourceCapability;
			}
			YarnServiceProtos.GetNewApplicationResponseProtoOrBuilder p = viaProto ? proto : 
				builder;
			if (!p.HasMaximumCapability())
			{
				return null;
			}
			this.maximumResourceCapability = ConvertFromProtoFormat(p.GetMaximumCapability());
			return this.maximumResourceCapability;
		}

		public override void SetMaximumResourceCapability(Resource capability)
		{
			MaybeInitBuilder();
			if (maximumResourceCapability == null)
			{
				builder.ClearMaximumCapability();
			}
			this.maximumResourceCapability = capability;
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

		private Resource ConvertFromProtoFormat(YarnProtos.ResourceProto resource)
		{
			return new ResourcePBImpl(resource);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource resource)
		{
			return ((ResourcePBImpl)resource).GetProto();
		}
	}
}
