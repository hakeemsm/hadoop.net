using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class KillApplicationResponsePBImpl : KillApplicationResponse
	{
		internal YarnServiceProtos.KillApplicationResponseProto proto = YarnServiceProtos.KillApplicationResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.KillApplicationResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public KillApplicationResponsePBImpl()
		{
			builder = YarnServiceProtos.KillApplicationResponseProto.NewBuilder();
		}

		public KillApplicationResponsePBImpl(YarnServiceProtos.KillApplicationResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.KillApplicationResponseProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.KillApplicationResponseProto)builder
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

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.KillApplicationResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override bool GetIsKillCompleted()
		{
			YarnServiceProtos.KillApplicationResponseProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetIsKillCompleted();
		}

		public override void SetIsKillCompleted(bool isKillCompleted)
		{
			MaybeInitBuilder();
			builder.SetIsKillCompleted(isKillCompleted);
		}
	}
}
