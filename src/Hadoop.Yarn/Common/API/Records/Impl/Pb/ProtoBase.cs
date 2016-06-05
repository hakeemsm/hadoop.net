using Com.Google.Protobuf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public abstract class ProtoBase<T>
		where T : Message
	{
		public abstract T GetProto();

		//TODO Force a comparator?
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

		protected internal ByteBuffer ConvertFromProtoFormat(ByteString byteString)
		{
			return ProtoUtils.ConvertFromProtoFormat(byteString);
		}

		protected internal ByteString ConvertToProtoFormat(ByteBuffer byteBuffer)
		{
			return ProtoUtils.ConvertToProtoFormat(byteBuffer);
		}
	}
}
