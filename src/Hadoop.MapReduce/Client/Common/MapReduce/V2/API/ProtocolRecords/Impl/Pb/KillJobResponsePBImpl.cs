using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class KillJobResponsePBImpl : ProtoBase<MRServiceProtos.KillJobResponseProto
		>, KillJobResponse
	{
		internal MRServiceProtos.KillJobResponseProto proto = MRServiceProtos.KillJobResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.KillJobResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public KillJobResponsePBImpl()
		{
			builder = MRServiceProtos.KillJobResponseProto.NewBuilder();
		}

		public KillJobResponsePBImpl(MRServiceProtos.KillJobResponseProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.KillJobResponseProto GetProto()
		{
			proto = viaProto ? proto : ((MRServiceProtos.KillJobResponseProto)builder.Build()
				);
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.KillJobResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}
	}
}
