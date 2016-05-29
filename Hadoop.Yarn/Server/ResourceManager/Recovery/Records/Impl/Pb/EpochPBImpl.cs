using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records.Impl.PB
{
	public class EpochPBImpl : Epoch
	{
		internal YarnServerResourceManagerRecoveryProtos.EpochProto proto = YarnServerResourceManagerRecoveryProtos.EpochProto
			.GetDefaultInstance();

		internal YarnServerResourceManagerRecoveryProtos.EpochProto.Builder builder = null;

		internal bool viaProto = false;

		public EpochPBImpl()
		{
			builder = YarnServerResourceManagerRecoveryProtos.EpochProto.NewBuilder();
		}

		public EpochPBImpl(YarnServerResourceManagerRecoveryProtos.EpochProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerResourceManagerRecoveryProtos.EpochProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServerResourceManagerRecoveryProtos.EpochProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerResourceManagerRecoveryProtos.EpochProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override long GetEpoch()
		{
			YarnServerResourceManagerRecoveryProtos.EpochProtoOrBuilder p = viaProto ? proto : 
				builder;
			return p.GetEpoch();
		}

		public override void SetEpoch(long sequentialNumber)
		{
			MaybeInitBuilder();
			builder.SetEpoch(sequentialNumber);
		}
	}
}
