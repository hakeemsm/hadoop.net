using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ResourceOptionPBImpl : ResourceOption
	{
		internal YarnProtos.ResourceOptionProto proto = YarnProtos.ResourceOptionProto.GetDefaultInstance
			();

		internal YarnProtos.ResourceOptionProto.Builder builder = null;

		internal bool viaProto = false;

		public ResourceOptionPBImpl()
		{
			builder = YarnProtos.ResourceOptionProto.NewBuilder();
		}

		public ResourceOptionPBImpl(YarnProtos.ResourceOptionProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ResourceOptionProto GetProto()
		{
			proto = viaProto ? proto : ((YarnProtos.ResourceOptionProto)builder.Build());
			viaProto = true;
			return proto;
		}

		public override Resource GetResource()
		{
			YarnProtos.ResourceOptionProtoOrBuilder p = viaProto ? proto : builder;
			return ConvertFromProtoFormat(p.GetResource());
		}

		protected override void SetResource(Resource resource)
		{
			MaybeInitBuilder();
			builder.SetResource(ConvertToProtoFormat(resource));
		}

		public override int GetOverCommitTimeout()
		{
			YarnProtos.ResourceOptionProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetOverCommitTimeout();
		}

		protected override void SetOverCommitTimeout(int overCommitTimeout)
		{
			MaybeInitBuilder();
			builder.SetOverCommitTimeout(overCommitTimeout);
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ResourceOptionProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource resource)
		{
			return ((ResourcePBImpl)resource).GetProto();
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		protected override void Build()
		{
			proto = ((YarnProtos.ResourceOptionProto)builder.Build());
			viaProto = true;
			builder = null;
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
	}
}
