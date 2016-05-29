using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB
{
	public class MasterKeyPBImpl : ProtoBase<YarnServerCommonProtos.MasterKeyProto>, 
		MasterKey
	{
		internal YarnServerCommonProtos.MasterKeyProto proto = YarnServerCommonProtos.MasterKeyProto
			.GetDefaultInstance();

		internal YarnServerCommonProtos.MasterKeyProto.Builder builder = null;

		internal bool viaProto = false;

		public MasterKeyPBImpl()
		{
			builder = YarnServerCommonProtos.MasterKeyProto.NewBuilder();
		}

		public MasterKeyPBImpl(YarnServerCommonProtos.MasterKeyProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerCommonProtos.MasterKeyProto GetProto()
		{
			lock (this)
			{
				proto = viaProto ? proto : ((YarnServerCommonProtos.MasterKeyProto)builder.Build(
					));
				viaProto = true;
				return proto;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = YarnServerCommonProtos.MasterKeyProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public virtual int GetKeyId()
		{
			lock (this)
			{
				YarnServerCommonProtos.MasterKeyProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetKeyId());
			}
		}

		public virtual void SetKeyId(int id)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetKeyId((id));
			}
		}

		public virtual ByteBuffer GetBytes()
		{
			lock (this)
			{
				YarnServerCommonProtos.MasterKeyProtoOrBuilder p = viaProto ? proto : builder;
				return ConvertFromProtoFormat(p.GetBytes());
			}
		}

		public virtual void SetBytes(ByteBuffer bytes)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetBytes(ConvertToProtoFormat(bytes));
			}
		}

		public override int GetHashCode()
		{
			return GetKeyId();
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (!(obj is MasterKey))
			{
				return false;
			}
			MasterKey other = (MasterKey)obj;
			if (this.GetKeyId() != other.GetKeyId())
			{
				return false;
			}
			if (!this.GetBytes().Equals(other.GetBytes()))
			{
				return false;
			}
			return true;
		}
	}
}
