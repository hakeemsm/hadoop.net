using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class KillTaskResponsePBImpl : ProtoBase<MRServiceProtos.KillTaskResponseProto
		>, KillTaskResponse
	{
		internal MRServiceProtos.KillTaskResponseProto proto = MRServiceProtos.KillTaskResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.KillTaskResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public KillTaskResponsePBImpl()
		{
			builder = MRServiceProtos.KillTaskResponseProto.NewBuilder();
		}

		public KillTaskResponsePBImpl(MRServiceProtos.KillTaskResponseProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.KillTaskResponseProto GetProto()
		{
			proto = viaProto ? proto : ((MRServiceProtos.KillTaskResponseProto)builder.Build(
				));
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.KillTaskResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}
	}
}
