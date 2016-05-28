using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class KillTaskAttemptResponsePBImpl : ProtoBase<MRServiceProtos.KillTaskAttemptResponseProto
		>, KillTaskAttemptResponse
	{
		internal MRServiceProtos.KillTaskAttemptResponseProto proto = MRServiceProtos.KillTaskAttemptResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.KillTaskAttemptResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public KillTaskAttemptResponsePBImpl()
		{
			builder = MRServiceProtos.KillTaskAttemptResponseProto.NewBuilder();
		}

		public KillTaskAttemptResponsePBImpl(MRServiceProtos.KillTaskAttemptResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.KillTaskAttemptResponseProto GetProto()
		{
			proto = viaProto ? proto : ((MRServiceProtos.KillTaskAttemptResponseProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.KillTaskAttemptResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}
	}
}
