using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class CountersPBImpl : ProtoBase<MRProtos.CountersProto>, Counters
	{
		internal MRProtos.CountersProto proto = MRProtos.CountersProto.GetDefaultInstance
			();

		internal MRProtos.CountersProto.Builder builder = null;

		internal bool viaProto = false;

		private IDictionary<string, CounterGroup> counterGroups = null;

		public CountersPBImpl()
		{
			builder = MRProtos.CountersProto.NewBuilder();
		}

		public CountersPBImpl(MRProtos.CountersProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRProtos.CountersProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRProtos.CountersProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.counterGroups != null)
			{
				AddCounterGroupsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRProtos.CountersProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRProtos.CountersProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual IDictionary<string, CounterGroup> GetAllCounterGroups()
		{
			InitCounterGroups();
			return this.counterGroups;
		}

		public virtual CounterGroup GetCounterGroup(string key)
		{
			InitCounterGroups();
			return this.counterGroups[key];
		}

		public virtual Counter GetCounter<_T0>(Enum<_T0> key)
			where _T0 : Enum<E>
		{
			CounterGroup group = GetCounterGroup(key.GetDeclaringClass().FullName);
			return group == null ? null : group.GetCounter(key.Name());
		}

		public virtual void IncrCounter<_T0>(Enum<_T0> key, long amount)
			where _T0 : Enum<E>
		{
			string groupName = key.GetDeclaringClass().FullName;
			if (GetCounterGroup(groupName) == null)
			{
				CounterGroup cGrp = new CounterGroupPBImpl();
				cGrp.SetName(groupName);
				cGrp.SetDisplayName(groupName);
				SetCounterGroup(groupName, cGrp);
			}
			if (GetCounterGroup(groupName).GetCounter(key.Name()) == null)
			{
				Counter c = new CounterPBImpl();
				c.SetName(key.Name());
				c.SetDisplayName(key.Name());
				c.SetValue(0l);
				GetCounterGroup(groupName).SetCounter(key.Name(), c);
			}
			Counter counter = GetCounterGroup(groupName).GetCounter(key.Name());
			counter.SetValue(counter.GetValue() + amount);
		}

		private void InitCounterGroups()
		{
			if (this.counterGroups != null)
			{
				return;
			}
			MRProtos.CountersProtoOrBuilder p = viaProto ? proto : builder;
			IList<MRProtos.StringCounterGroupMapProto> list = p.GetCounterGroupsList();
			this.counterGroups = new Dictionary<string, CounterGroup>();
			foreach (MRProtos.StringCounterGroupMapProto c in list)
			{
				this.counterGroups[c.GetKey()] = ConvertFromProtoFormat(c.GetValue());
			}
		}

		public virtual void AddAllCounterGroups(IDictionary<string, CounterGroup> counterGroups
			)
		{
			if (counterGroups == null)
			{
				return;
			}
			InitCounterGroups();
			this.counterGroups.PutAll(counterGroups);
		}

		private void AddCounterGroupsToProto()
		{
			MaybeInitBuilder();
			builder.ClearCounterGroups();
			if (counterGroups == null)
			{
				return;
			}
			IEnumerable<MRProtos.StringCounterGroupMapProto> iterable = new _IEnumerable_146(
				this);
			builder.AddAllCounterGroups(iterable);
		}

		private sealed class _IEnumerable_146 : IEnumerable<MRProtos.StringCounterGroupMapProto
			>
		{
			public _IEnumerable_146(CountersPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<MRProtos.StringCounterGroupMapProto> GetEnumerator()
			{
				return new _IEnumerator_150(this);
			}

			private sealed class _IEnumerator_150 : IEnumerator<MRProtos.StringCounterGroupMapProto
				>
			{
				public _IEnumerator_150(_IEnumerable_146 _enclosing)
				{
					this._enclosing = _enclosing;
					this.keyIter = this._enclosing._enclosing.counterGroups.Keys.GetEnumerator();
				}

				internal IEnumerator<string> keyIter;

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				public override MRProtos.StringCounterGroupMapProto Next()
				{
					string key = this.keyIter.Next();
					return ((MRProtos.StringCounterGroupMapProto)MRProtos.StringCounterGroupMapProto.
						NewBuilder().SetKey(key).SetValue(this._enclosing._enclosing.ConvertToProtoFormat
						(this._enclosing._enclosing.counterGroups[key])).Build());
				}

				public override bool HasNext()
				{
					return this.keyIter.HasNext();
				}

				private readonly _IEnumerable_146 _enclosing;
			}

			private readonly CountersPBImpl _enclosing;
		}

		public virtual void SetCounterGroup(string key, CounterGroup val)
		{
			InitCounterGroups();
			this.counterGroups[key] = val;
		}

		public virtual void RemoveCounterGroup(string key)
		{
			InitCounterGroups();
			Sharpen.Collections.Remove(this.counterGroups, key);
		}

		public virtual void ClearCounterGroups()
		{
			InitCounterGroups();
			this.counterGroups.Clear();
		}

		private CounterGroupPBImpl ConvertFromProtoFormat(MRProtos.CounterGroupProto p)
		{
			return new CounterGroupPBImpl(p);
		}

		private MRProtos.CounterGroupProto ConvertToProtoFormat(CounterGroup t)
		{
			return ((CounterGroupPBImpl)t).GetProto();
		}
	}
}
