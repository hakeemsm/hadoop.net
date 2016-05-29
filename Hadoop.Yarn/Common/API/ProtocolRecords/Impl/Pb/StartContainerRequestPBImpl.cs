using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class StartContainerRequestPBImpl : StartContainerRequest
	{
		internal YarnServiceProtos.StartContainerRequestProto proto = YarnServiceProtos.StartContainerRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.StartContainerRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerLaunchContext containerLaunchContext = null;

		private Token containerToken = null;

		public StartContainerRequestPBImpl()
		{
			builder = YarnServiceProtos.StartContainerRequestProto.NewBuilder();
		}

		public StartContainerRequestPBImpl(YarnServiceProtos.StartContainerRequestProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.StartContainerRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.StartContainerRequestProto)builder
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
			if (this.containerLaunchContext != null)
			{
				builder.SetContainerLaunchContext(ConvertToProtoFormat(this.containerLaunchContext
					));
			}
			if (this.containerToken != null)
			{
				builder.SetContainerToken(ConvertToProtoFormat(this.containerToken));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.StartContainerRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.StartContainerRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ContainerLaunchContext GetContainerLaunchContext()
		{
			YarnServiceProtos.StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.containerLaunchContext != null)
			{
				return this.containerLaunchContext;
			}
			if (!p.HasContainerLaunchContext())
			{
				return null;
			}
			this.containerLaunchContext = ConvertFromProtoFormat(p.GetContainerLaunchContext(
				));
			return this.containerLaunchContext;
		}

		public override void SetContainerLaunchContext(ContainerLaunchContext containerLaunchContext
			)
		{
			MaybeInitBuilder();
			if (containerLaunchContext == null)
			{
				builder.ClearContainerLaunchContext();
			}
			this.containerLaunchContext = containerLaunchContext;
		}

		public override Token GetContainerToken()
		{
			YarnServiceProtos.StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.containerToken != null)
			{
				return this.containerToken;
			}
			if (!p.HasContainerToken())
			{
				return null;
			}
			this.containerToken = ConvertFromProtoFormat(p.GetContainerToken());
			return this.containerToken;
		}

		public override void SetContainerToken(Token containerToken)
		{
			MaybeInitBuilder();
			if (containerToken == null)
			{
				builder.ClearContainerToken();
			}
			this.containerToken = containerToken;
		}

		private ContainerLaunchContextPBImpl ConvertFromProtoFormat(YarnProtos.ContainerLaunchContextProto
			 p)
		{
			return new ContainerLaunchContextPBImpl(p);
		}

		private YarnProtos.ContainerLaunchContextProto ConvertToProtoFormat(ContainerLaunchContext
			 t)
		{
			return ((ContainerLaunchContextPBImpl)t).GetProto();
		}

		private TokenPBImpl ConvertFromProtoFormat(SecurityProtos.TokenProto containerProto
			)
		{
			return new TokenPBImpl(containerProto);
		}

		private SecurityProtos.TokenProto ConvertToProtoFormat(Token container)
		{
			return ((TokenPBImpl)container).GetProto();
		}
	}
}
