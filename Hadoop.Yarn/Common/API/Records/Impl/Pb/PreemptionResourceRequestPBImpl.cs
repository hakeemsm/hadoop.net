using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class PreemptionResourceRequestPBImpl : PreemptionResourceRequest
	{
		internal YarnProtos.PreemptionResourceRequestProto proto = YarnProtos.PreemptionResourceRequestProto
			.GetDefaultInstance();

		internal YarnProtos.PreemptionResourceRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ResourceRequest rr;

		public PreemptionResourceRequestPBImpl()
		{
			builder = YarnProtos.PreemptionResourceRequestProto.NewBuilder();
		}

		public PreemptionResourceRequestPBImpl(YarnProtos.PreemptionResourceRequestProto 
			proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.PreemptionResourceRequestProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnProtos.PreemptionResourceRequestProto)builder.Build
					());
				viaProto = true;
				return proto;
			}
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

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.PreemptionResourceRequestProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (rr != null)
			{
				builder.SetResource(ConvertToProtoFormat(rr));
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.PreemptionResourceRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ResourceRequest GetResourceRequest()
		{
			lock (this)
			{
				YarnProtos.PreemptionResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
				if (rr != null)
				{
					return rr;
				}
				if (!p.HasResource())
				{
					return null;
				}
				rr = ConvertFromProtoFormat(p.GetResource());
				return rr;
			}
		}

		public override void SetResourceRequest(ResourceRequest rr)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (null == rr)
				{
					builder.ClearResource();
				}
				this.rr = rr;
			}
		}

		private ResourceRequestPBImpl ConvertFromProtoFormat(YarnProtos.ResourceRequestProto
			 p)
		{
			return new ResourceRequestPBImpl(p);
		}

		private YarnProtos.ResourceRequestProto ConvertToProtoFormat(ResourceRequest t)
		{
			return ((ResourceRequestPBImpl)t).GetProto();
		}
	}
}
