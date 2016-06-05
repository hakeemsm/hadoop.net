using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records.Impl.PB
{
	public class AMRMTokenSecretManagerStatePBImpl : AMRMTokenSecretManagerState
	{
		internal YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
			 proto = YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
			.GetDefaultInstance();

		internal YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto.Builder
			 builder = null;

		internal bool viaProto = false;

		private MasterKey currentMasterKey = null;

		private MasterKey nextMasterKey = null;

		public AMRMTokenSecretManagerStatePBImpl()
		{
			builder = YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
				.NewBuilder();
		}

		public AMRMTokenSecretManagerStatePBImpl(YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
			 GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.currentMasterKey != null)
			{
				builder.SetCurrentMasterKey(ConvertToProtoFormat(this.currentMasterKey));
			}
			if (this.nextMasterKey != null)
			{
				builder.SetNextMasterKey(ConvertToProtoFormat(this.nextMasterKey));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
				)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
					.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override MasterKey GetCurrentMasterKey()
		{
			YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProtoOrBuilder
				 p = viaProto ? proto : builder;
			if (this.currentMasterKey != null)
			{
				return this.currentMasterKey;
			}
			if (!p.HasCurrentMasterKey())
			{
				return null;
			}
			this.currentMasterKey = ConvertFromProtoFormat(p.GetCurrentMasterKey());
			return this.currentMasterKey;
		}

		public override void SetCurrentMasterKey(MasterKey currentMasterKey)
		{
			MaybeInitBuilder();
			if (currentMasterKey == null)
			{
				builder.ClearCurrentMasterKey();
			}
			this.currentMasterKey = currentMasterKey;
		}

		public override MasterKey GetNextMasterKey()
		{
			YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProtoOrBuilder
				 p = viaProto ? proto : builder;
			if (this.nextMasterKey != null)
			{
				return this.nextMasterKey;
			}
			if (!p.HasNextMasterKey())
			{
				return null;
			}
			this.nextMasterKey = ConvertFromProtoFormat(p.GetNextMasterKey());
			return this.nextMasterKey;
		}

		public override void SetNextMasterKey(MasterKey nextMasterKey)
		{
			MaybeInitBuilder();
			if (nextMasterKey == null)
			{
				builder.ClearNextMasterKey();
			}
			this.nextMasterKey = nextMasterKey;
		}

		private YarnServerCommonProtos.MasterKeyProto ConvertToProtoFormat(MasterKey t)
		{
			return ((MasterKeyPBImpl)t).GetProto();
		}

		private MasterKeyPBImpl ConvertFromProtoFormat(YarnServerCommonProtos.MasterKeyProto
			 p)
		{
			return new MasterKeyPBImpl(p);
		}
	}
}
