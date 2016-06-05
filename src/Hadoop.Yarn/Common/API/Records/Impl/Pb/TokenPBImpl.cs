using System.Text;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class TokenPBImpl : Token
	{
		private SecurityProtos.TokenProto proto = SecurityProtos.TokenProto.GetDefaultInstance
			();

		private SecurityProtos.TokenProto.Builder builder = null;

		private bool viaProto = false;

		private ByteBuffer identifier;

		private ByteBuffer password;

		public TokenPBImpl()
		{
			builder = SecurityProtos.TokenProto.NewBuilder();
		}

		public TokenPBImpl(SecurityProtos.TokenProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual SecurityProtos.TokenProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((SecurityProtos.TokenProto)builder.Build());
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

		protected internal ByteBuffer ConvertFromProtoFormat(ByteString byteString)
		{
			return ProtoUtils.ConvertFromProtoFormat(byteString);
		}

		protected internal ByteString ConvertToProtoFormat(ByteBuffer byteBuffer)
		{
			return ProtoUtils.ConvertToProtoFormat(byteBuffer);
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.identifier != null)
				{
					builder.SetIdentifier(ConvertToProtoFormat(this.identifier));
				}
				if (this.password != null)
				{
					builder.SetPassword(ConvertToProtoFormat(this.password));
				}
			}
		}

		private void MergeLocalToProto()
		{
			lock (this)
			{
				if (viaProto)
				{
					MaybeInitBuilder();
				}
				MergeLocalToBuilder();
				proto = ((SecurityProtos.TokenProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = SecurityProtos.TokenProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public override ByteBuffer GetIdentifier()
		{
			lock (this)
			{
				SecurityProtos.TokenProtoOrBuilder p = viaProto ? proto : builder;
				if (this.identifier != null)
				{
					return this.identifier;
				}
				if (!p.HasIdentifier())
				{
					return null;
				}
				this.identifier = ConvertFromProtoFormat(p.GetIdentifier());
				return this.identifier;
			}
		}

		public override void SetIdentifier(ByteBuffer identifier)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (identifier == null)
				{
					builder.ClearIdentifier();
				}
				this.identifier = identifier;
			}
		}

		public override ByteBuffer GetPassword()
		{
			lock (this)
			{
				SecurityProtos.TokenProtoOrBuilder p = viaProto ? proto : builder;
				if (this.password != null)
				{
					return this.password;
				}
				if (!p.HasPassword())
				{
					return null;
				}
				this.password = ConvertFromProtoFormat(p.GetPassword());
				return this.password;
			}
		}

		public override void SetPassword(ByteBuffer password)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (password == null)
				{
					builder.ClearPassword();
				}
				this.password = password;
			}
		}

		public override string GetKind()
		{
			lock (this)
			{
				SecurityProtos.TokenProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasKind())
				{
					return null;
				}
				return (p.GetKind());
			}
		}

		public override void SetKind(string kind)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (kind == null)
				{
					builder.ClearKind();
					return;
				}
				builder.SetKind((kind));
			}
		}

		public override string GetService()
		{
			lock (this)
			{
				SecurityProtos.TokenProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasService())
				{
					return null;
				}
				return (p.GetService());
			}
		}

		public override void SetService(string service)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (service == null)
				{
					builder.ClearService();
					return;
				}
				builder.SetService((service));
			}
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("Token { ");
			sb.Append("kind: ").Append(GetKind()).Append(", ");
			sb.Append("service: ").Append(GetService()).Append(" }");
			return sb.ToString();
		}
	}
}
