using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class URLPBImpl : URL
	{
		internal YarnProtos.URLProto proto = YarnProtos.URLProto.GetDefaultInstance();

		internal YarnProtos.URLProto.Builder builder = null;

		internal bool viaProto = false;

		public URLPBImpl()
		{
			builder = YarnProtos.URLProto.NewBuilder();
		}

		public URLPBImpl(YarnProtos.URLProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.URLProto GetProto()
		{
			proto = viaProto ? proto : ((YarnProtos.URLProto)builder.Build());
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
			return TextFormat.ShortDebugString(GetProto());
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.URLProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override string GetFile()
		{
			YarnProtos.URLProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasFile())
			{
				return null;
			}
			return (p.GetFile());
		}

		public override void SetFile(string file)
		{
			MaybeInitBuilder();
			if (file == null)
			{
				builder.ClearFile();
				return;
			}
			builder.SetFile((file));
		}

		public override string GetScheme()
		{
			YarnProtos.URLProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasScheme())
			{
				return null;
			}
			return (p.GetScheme());
		}

		public override void SetScheme(string scheme)
		{
			MaybeInitBuilder();
			if (scheme == null)
			{
				builder.ClearScheme();
				return;
			}
			builder.SetScheme((scheme));
		}

		public override string GetUserInfo()
		{
			YarnProtos.URLProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasUserInfo())
			{
				return null;
			}
			return (p.GetUserInfo());
		}

		public override void SetUserInfo(string userInfo)
		{
			MaybeInitBuilder();
			if (userInfo == null)
			{
				builder.ClearUserInfo();
				return;
			}
			builder.SetUserInfo((userInfo));
		}

		public override string GetHost()
		{
			YarnProtos.URLProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasHost())
			{
				return null;
			}
			return (p.GetHost());
		}

		public override void SetHost(string host)
		{
			MaybeInitBuilder();
			if (host == null)
			{
				builder.ClearHost();
				return;
			}
			builder.SetHost((host));
		}

		public override int GetPort()
		{
			YarnProtos.URLProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetPort());
		}

		public override void SetPort(int port)
		{
			MaybeInitBuilder();
			builder.SetPort((port));
		}
	}
}
