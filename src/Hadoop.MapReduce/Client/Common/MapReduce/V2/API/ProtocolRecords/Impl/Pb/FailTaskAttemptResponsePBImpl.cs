using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class FailTaskAttemptResponsePBImpl : ProtoBase<MRServiceProtos.FailTaskAttemptResponseProto
		>, FailTaskAttemptResponse
	{
		internal MRServiceProtos.FailTaskAttemptResponseProto proto = MRServiceProtos.FailTaskAttemptResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.FailTaskAttemptResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public FailTaskAttemptResponsePBImpl()
		{
			builder = MRServiceProtos.FailTaskAttemptResponseProto.NewBuilder();
		}

		public FailTaskAttemptResponsePBImpl(MRServiceProtos.FailTaskAttemptResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.FailTaskAttemptResponseProto GetProto()
		{
			proto = viaProto ? proto : ((MRServiceProtos.FailTaskAttemptResponseProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.FailTaskAttemptResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}
	}
}
