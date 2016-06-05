using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class CounterPBImpl : ProtoBase<MRProtos.CounterProto>, Counter
	{
		internal MRProtos.CounterProto proto = MRProtos.CounterProto.GetDefaultInstance();

		internal MRProtos.CounterProto.Builder builder = null;

		internal bool viaProto = false;

		public CounterPBImpl()
		{
			builder = MRProtos.CounterProto.NewBuilder();
		}

		public CounterPBImpl(MRProtos.CounterProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRProtos.CounterProto GetProto()
		{
			proto = viaProto ? proto : ((MRProtos.CounterProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRProtos.CounterProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual string GetName()
		{
			MRProtos.CounterProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasName())
			{
				return null;
			}
			return (p.GetName());
		}

		public virtual void SetName(string name)
		{
			MaybeInitBuilder();
			if (name == null)
			{
				builder.ClearName();
				return;
			}
			builder.SetName((name));
		}

		public virtual long GetValue()
		{
			MRProtos.CounterProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetValue());
		}

		public virtual void SetValue(long value)
		{
			MaybeInitBuilder();
			builder.SetValue((value));
		}

		public virtual string GetDisplayName()
		{
			MRProtos.CounterProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasDisplayName())
			{
				return null;
			}
			return (p.GetDisplayName());
		}

		public virtual void SetDisplayName(string displayName)
		{
			MaybeInitBuilder();
			if (displayName == null)
			{
				builder.ClearDisplayName();
				return;
			}
			builder.SetDisplayName((displayName));
		}
	}
}
