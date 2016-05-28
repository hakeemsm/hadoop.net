using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class CounterGroupPBImpl : ProtoBase<MRProtos.CounterGroupProto>, CounterGroup
	{
		internal MRProtos.CounterGroupProto proto = MRProtos.CounterGroupProto.GetDefaultInstance
			();

		internal MRProtos.CounterGroupProto.Builder builder = null;

		internal bool viaProto = false;

		private IDictionary<string, Counter> counters = null;

		public CounterGroupPBImpl()
		{
			builder = MRProtos.CounterGroupProto.NewBuilder();
		}

		public CounterGroupPBImpl(MRProtos.CounterGroupProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRProtos.CounterGroupProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRProtos.CounterGroupProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.counters != null)
			{
				AddContersToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRProtos.CounterGroupProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRProtos.CounterGroupProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual string GetName()
		{
			MRProtos.CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
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

		public virtual string GetDisplayName()
		{
			MRProtos.CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
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

		public virtual IDictionary<string, Counter> GetAllCounters()
		{
			InitCounters();
			return this.counters;
		}

		public virtual Counter GetCounter(string key)
		{
			InitCounters();
			return this.counters[key];
		}

		private void InitCounters()
		{
			if (this.counters != null)
			{
				return;
			}
			MRProtos.CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
			IList<MRProtos.StringCounterMapProto> list = p.GetCountersList();
			this.counters = new Dictionary<string, Counter>();
			foreach (MRProtos.StringCounterMapProto c in list)
			{
				this.counters[c.GetKey()] = ConvertFromProtoFormat(c.GetValue());
			}
		}

		public virtual void AddAllCounters(IDictionary<string, Counter> counters)
		{
			if (counters == null)
			{
				return;
			}
			InitCounters();
			this.counters.PutAll(counters);
		}

		private void AddContersToProto()
		{
			MaybeInitBuilder();
			builder.ClearCounters();
			if (counters == null)
			{
				return;
			}
			IEnumerable<MRProtos.StringCounterMapProto> iterable = new _IEnumerable_156(this);
			builder.AddAllCounters(iterable);
		}

		private sealed class _IEnumerable_156 : IEnumerable<MRProtos.StringCounterMapProto
			>
		{
			public _IEnumerable_156(CounterGroupPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<MRProtos.StringCounterMapProto> GetEnumerator()
			{
				return new _IEnumerator_160(this);
			}

			private sealed class _IEnumerator_160 : IEnumerator<MRProtos.StringCounterMapProto
				>
			{
				public _IEnumerator_160(_IEnumerable_156 _enclosing)
				{
					this._enclosing = _enclosing;
					this.keyIter = this._enclosing._enclosing.counters.Keys.GetEnumerator();
				}

				internal IEnumerator<string> keyIter;

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				public override MRProtos.StringCounterMapProto Next()
				{
					string key = this.keyIter.Next();
					return ((MRProtos.StringCounterMapProto)MRProtos.StringCounterMapProto.NewBuilder
						().SetKey(key).SetValue(this._enclosing._enclosing.ConvertToProtoFormat(this._enclosing
						._enclosing.counters[key])).Build());
				}

				public override bool HasNext()
				{
					return this.keyIter.HasNext();
				}

				private readonly _IEnumerable_156 _enclosing;
			}

			private readonly CounterGroupPBImpl _enclosing;
		}

		public virtual void SetCounter(string key, Counter val)
		{
			InitCounters();
			this.counters[key] = val;
		}

		public virtual void RemoveCounter(string key)
		{
			InitCounters();
			Sharpen.Collections.Remove(this.counters, key);
		}

		public virtual void ClearCounters()
		{
			InitCounters();
			this.counters.Clear();
		}

		private CounterPBImpl ConvertFromProtoFormat(MRProtos.CounterProto p)
		{
			return new CounterPBImpl(p);
		}

		private MRProtos.CounterProto ConvertToProtoFormat(Counter t)
		{
			return ((CounterPBImpl)t).GetProto();
		}
	}
}
