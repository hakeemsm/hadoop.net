using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB
{
	public class VersionPBImpl : Version
	{
		internal YarnServerCommonProtos.VersionProto proto = YarnServerCommonProtos.VersionProto
			.GetDefaultInstance();

		internal YarnServerCommonProtos.VersionProto.Builder builder = null;

		internal bool viaProto = false;

		public VersionPBImpl()
		{
			builder = YarnServerCommonProtos.VersionProto.NewBuilder();
		}

		public VersionPBImpl(YarnServerCommonProtos.VersionProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonProtos.VersionProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServerCommonProtos.VersionProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonProtos.VersionProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override int GetMajorVersion()
		{
			YarnServerCommonProtos.VersionProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetMajorVersion();
		}

		public override void SetMajorVersion(int major)
		{
			MaybeInitBuilder();
			builder.SetMajorVersion(major);
		}

		public override int GetMinorVersion()
		{
			YarnServerCommonProtos.VersionProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetMinorVersion();
		}

		public override void SetMinorVersion(int minor)
		{
			MaybeInitBuilder();
			builder.SetMinorVersion(minor);
		}
	}
}
