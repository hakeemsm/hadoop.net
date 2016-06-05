using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class RegisterApplicationMasterRequestPBImpl : RegisterApplicationMasterRequest
	{
		internal YarnServiceProtos.RegisterApplicationMasterRequestProto proto = YarnServiceProtos.RegisterApplicationMasterRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.RegisterApplicationMasterRequestProto.Builder builder = 
			null;

		internal bool viaProto = false;

		public RegisterApplicationMasterRequestPBImpl()
		{
			builder = YarnServiceProtos.RegisterApplicationMasterRequestProto.NewBuilder();
		}

		public RegisterApplicationMasterRequestPBImpl(YarnServiceProtos.RegisterApplicationMasterRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.RegisterApplicationMasterRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.RegisterApplicationMasterRequestProto
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
			proto = ((YarnServiceProtos.RegisterApplicationMasterRequestProto)builder.Build()
				);
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.RegisterApplicationMasterRequestProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		public override string GetHost()
		{
			YarnServiceProtos.RegisterApplicationMasterRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetHost();
		}

		public override void SetHost(string host)
		{
			MaybeInitBuilder();
			if (host == null)
			{
				builder.ClearHost();
				return;
			}
			builder.SetHost(host);
		}

		public override int GetRpcPort()
		{
			YarnServiceProtos.RegisterApplicationMasterRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetRpcPort();
		}

		public override void SetRpcPort(int port)
		{
			MaybeInitBuilder();
			builder.SetRpcPort(port);
		}

		public override string GetTrackingUrl()
		{
			YarnServiceProtos.RegisterApplicationMasterRequestProtoOrBuilder p = viaProto ? proto
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
	}
}
