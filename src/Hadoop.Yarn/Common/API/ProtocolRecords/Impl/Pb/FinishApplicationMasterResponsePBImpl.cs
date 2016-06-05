using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class FinishApplicationMasterResponsePBImpl : FinishApplicationMasterResponse
	{
		internal YarnServiceProtos.FinishApplicationMasterResponseProto proto = YarnServiceProtos.FinishApplicationMasterResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.FinishApplicationMasterResponseProto.Builder builder = 
			null;

		internal bool viaProto = false;

		public FinishApplicationMasterResponsePBImpl()
		{
			builder = YarnServiceProtos.FinishApplicationMasterResponseProto.NewBuilder();
		}

		public FinishApplicationMasterResponsePBImpl(YarnServiceProtos.FinishApplicationMasterResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.FinishApplicationMasterResponseProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.FinishApplicationMasterResponseProto
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

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.FinishApplicationMasterResponseProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		public override bool GetIsUnregistered()
		{
			YarnServiceProtos.FinishApplicationMasterResponseProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetIsUnregistered();
		}

		public override void SetIsUnregistered(bool isUnregistered)
		{
			MaybeInitBuilder();
			builder.SetIsUnregistered(isUnregistered);
		}
	}
}
