using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetCountersResponsePBImpl : ProtoBase<MRServiceProtos.GetCountersResponseProto
		>, GetCountersResponse
	{
		internal MRServiceProtos.GetCountersResponseProto proto = MRServiceProtos.GetCountersResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetCountersResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private Counters counters = null;

		public GetCountersResponsePBImpl()
		{
			builder = MRServiceProtos.GetCountersResponseProto.NewBuilder();
		}

		public GetCountersResponsePBImpl(MRServiceProtos.GetCountersResponseProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetCountersResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetCountersResponseProto)builder.Build
				());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.counters != null)
			{
				builder.SetCounters(ConvertToProtoFormat(this.counters));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.GetCountersResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetCountersResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual Counters GetCounters()
		{
			MRServiceProtos.GetCountersResponseProtoOrBuilder p = viaProto ? proto : builder;
			if (this.counters != null)
			{
				return this.counters;
			}
			if (!p.HasCounters())
			{
				return null;
			}
			this.counters = ConvertFromProtoFormat(p.GetCounters());
			return this.counters;
		}

		public virtual void SetCounters(Counters counters)
		{
			MaybeInitBuilder();
			if (counters == null)
			{
				builder.ClearCounters();
			}
			this.counters = counters;
		}

		private CountersPBImpl ConvertFromProtoFormat(MRProtos.CountersProto p)
		{
			return new CountersPBImpl(p);
		}

		private MRProtos.CountersProto ConvertToProtoFormat(Counters t)
		{
			return ((CountersPBImpl)t).GetProto();
		}
	}
}
