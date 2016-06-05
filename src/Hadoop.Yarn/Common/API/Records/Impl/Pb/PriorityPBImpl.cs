using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class PriorityPBImpl : Priority
	{
		internal YarnProtos.PriorityProto proto = YarnProtos.PriorityProto.GetDefaultInstance
			();

		internal YarnProtos.PriorityProto.Builder builder = null;

		internal bool viaProto = false;

		public PriorityPBImpl()
		{
			builder = YarnProtos.PriorityProto.NewBuilder();
		}

		public PriorityPBImpl(YarnProtos.PriorityProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.PriorityProto GetProto()
		{
			proto = viaProto ? proto : ((YarnProtos.PriorityProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.PriorityProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override int GetPriority()
		{
			YarnProtos.PriorityProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetPriority());
		}

		public override void SetPriority(int priority)
		{
			MaybeInitBuilder();
			builder.SetPriority((priority));
		}

		public override string ToString()
		{
			return Sharpen.Extensions.ToString(GetPriority());
		}
	}
}
