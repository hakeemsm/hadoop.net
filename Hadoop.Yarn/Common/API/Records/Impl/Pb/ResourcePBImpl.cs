using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ResourcePBImpl : Resource
	{
		internal YarnProtos.ResourceProto proto = YarnProtos.ResourceProto.GetDefaultInstance
			();

		internal YarnProtos.ResourceProto.Builder builder = null;

		internal bool viaProto = false;

		public ResourcePBImpl()
		{
			builder = YarnProtos.ResourceProto.NewBuilder();
		}

		public ResourcePBImpl(YarnProtos.ResourceProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ResourceProto GetProto()
		{
			proto = viaProto ? proto : ((YarnProtos.ResourceProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ResourceProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override int GetMemory()
		{
			YarnProtos.ResourceProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetMemory());
		}

		public override void SetMemory(int memory)
		{
			MaybeInitBuilder();
			builder.SetMemory((memory));
		}

		public override int GetVirtualCores()
		{
			YarnProtos.ResourceProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetVirtualCores());
		}

		public override void SetVirtualCores(int vCores)
		{
			MaybeInitBuilder();
			builder.SetVirtualCores((vCores));
		}

		public override int CompareTo(Resource other)
		{
			int diff = this.GetMemory() - other.GetMemory();
			if (diff == 0)
			{
				diff = this.GetVirtualCores() - other.GetVirtualCores();
			}
			return diff;
		}
	}
}
