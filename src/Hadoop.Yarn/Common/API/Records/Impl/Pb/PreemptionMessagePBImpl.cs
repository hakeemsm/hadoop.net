using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class PreemptionMessagePBImpl : PreemptionMessage
	{
		internal YarnProtos.PreemptionMessageProto proto = YarnProtos.PreemptionMessageProto
			.GetDefaultInstance();

		internal YarnProtos.PreemptionMessageProto.Builder builder = null;

		internal bool viaProto = false;

		private StrictPreemptionContract strict;

		private PreemptionContract contract;

		public PreemptionMessagePBImpl()
		{
			builder = YarnProtos.PreemptionMessageProto.NewBuilder();
		}

		public PreemptionMessagePBImpl(YarnProtos.PreemptionMessageProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.PreemptionMessageProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnProtos.PreemptionMessageProto)builder.Build());
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
			proto = ((YarnProtos.PreemptionMessageProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (strict != null)
			{
				builder.SetStrictContract(ConvertToProtoFormat(strict));
			}
			if (contract != null)
			{
				builder.SetContract(ConvertToProtoFormat(contract));
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.PreemptionMessageProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override StrictPreemptionContract GetStrictContract()
		{
			lock (this)
			{
				YarnProtos.PreemptionMessageProtoOrBuilder p = viaProto ? proto : builder;
				if (strict != null)
				{
					return strict;
				}
				if (!p.HasStrictContract())
				{
					return null;
				}
				strict = ConvertFromProtoFormat(p.GetStrictContract());
				return strict;
			}
		}

		public override void SetStrictContract(StrictPreemptionContract strict)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (null == strict)
				{
					builder.ClearStrictContract();
				}
				this.strict = strict;
			}
		}

		public override PreemptionContract GetContract()
		{
			lock (this)
			{
				YarnProtos.PreemptionMessageProtoOrBuilder p = viaProto ? proto : builder;
				if (contract != null)
				{
					return contract;
				}
				if (!p.HasContract())
				{
					return null;
				}
				contract = ConvertFromProtoFormat(p.GetContract());
				return contract;
			}
		}

		public override void SetContract(PreemptionContract c)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (null == c)
				{
					builder.ClearContract();
				}
				this.contract = c;
			}
		}

		private StrictPreemptionContractPBImpl ConvertFromProtoFormat(YarnProtos.StrictPreemptionContractProto
			 p)
		{
			return new StrictPreemptionContractPBImpl(p);
		}

		private YarnProtos.StrictPreemptionContractProto ConvertToProtoFormat(StrictPreemptionContract
			 t)
		{
			return ((StrictPreemptionContractPBImpl)t).GetProto();
		}

		private PreemptionContractPBImpl ConvertFromProtoFormat(YarnProtos.PreemptionContractProto
			 p)
		{
			return new PreemptionContractPBImpl(p);
		}

		private YarnProtos.PreemptionContractProto ConvertToProtoFormat(PreemptionContract
			 t)
		{
			return ((PreemptionContractPBImpl)t).GetProto();
		}
	}
}
