using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class UpdateNodeResourceResponsePBImpl : UpdateNodeResourceResponse
	{
		internal YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto proto
			 = YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto.GetDefaultInstance
			();

		internal YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto.Builder
			 builder = null;

		internal bool viaProto = false;

		public UpdateNodeResourceResponsePBImpl()
		{
			builder = YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto.
				NewBuilder();
		}

		public UpdateNodeResourceResponsePBImpl(YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto
			 GetProto()
		{
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto
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
			return GetProto().ToString().ReplaceAll("\\n", ", ").ReplaceAll("\\s+", " ");
		}
	}
}
