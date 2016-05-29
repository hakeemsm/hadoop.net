using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetDelegationTokenRequestPBImpl : GetDelegationTokenRequest
	{
		internal string renewer;

		internal SecurityProtos.GetDelegationTokenRequestProto proto = SecurityProtos.GetDelegationTokenRequestProto
			.GetDefaultInstance();

		internal SecurityProtos.GetDelegationTokenRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetDelegationTokenRequestPBImpl()
		{
			builder = SecurityProtos.GetDelegationTokenRequestProto.NewBuilder();
		}

		public GetDelegationTokenRequestPBImpl(SecurityProtos.GetDelegationTokenRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override string GetRenewer()
		{
			SecurityProtos.GetDelegationTokenRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.renewer != null)
			{
				return this.renewer;
			}
			this.renewer = p.GetRenewer();
			return this.renewer;
		}

		public override void SetRenewer(string renewer)
		{
			MaybeInitBuilder();
			if (renewer == null)
			{
				builder.ClearRenewer();
			}
			this.renewer = renewer;
		}

		public virtual SecurityProtos.GetDelegationTokenRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((SecurityProtos.GetDelegationTokenRequestProto)builder
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
			if (renewer != null)
			{
				builder.SetRenewer(this.renewer);
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((SecurityProtos.GetDelegationTokenRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = SecurityProtos.GetDelegationTokenRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}
	}
}
