using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Helpers to create interned metrics info</summary>
	public class Interns
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Interns));

		private abstract class CacheWith2Keys<K1, K2, V>
		{
			private sealed class _LinkedHashMap_43 : LinkedHashMap<K1, IDictionary<K2, V>>
			{
				public _LinkedHashMap_43(CacheWith2Keys<K1, K2, V> _enclosing)
				{
					this._enclosing = _enclosing;
					this.serialVersionUID = 1L;
					this.gotOverflow = false;
				}

				private const long serialVersionUID;

				private bool gotOverflow;

				// A simple intern cache with two keys
				// (to avoid creating new (combined) key objects for lookup)
				protected override bool RemoveEldestEntry(KeyValuePair<K1, IDictionary<K2, V>> e)
				{
					bool overflow = this._enclosing.ExpireKey1At(this._enclosing._enclosing._enclosing
						.Count);
					if (overflow && !this.gotOverflow)
					{
						Interns.Log.Warn("Metrics intern cache overflow at " + this._enclosing._enclosing
							._enclosing.Count + " for " + e);
						this.gotOverflow = true;
					}
					return overflow;
				}

				private readonly CacheWith2Keys<K1, K2, V> _enclosing;
			}

			private readonly IDictionary<K1, IDictionary<K2, V>> k1Map;

			protected internal abstract bool ExpireKey1At(int size);

			protected internal abstract bool ExpireKey2At(int size);

			protected internal abstract V NewValue(K1 k1, K2 k2);

			internal virtual V Add(K1 k1, K2 k2)
			{
				lock (this)
				{
					IDictionary<K2, V> k2Map = k1Map[k1];
					if (k2Map == null)
					{
						k2Map = new _LinkedHashMap_64(this);
						k1Map[k1] = k2Map;
					}
					V v = k2Map[k2];
					if (v == null)
					{
						v = NewValue(k1, k2);
						k2Map[k2] = v;
					}
					return v;
				}
			}

			private sealed class _LinkedHashMap_64 : LinkedHashMap<K2, V>
			{
				public _LinkedHashMap_64(CacheWith2Keys<K1, K2, V> _enclosing)
				{
					this._enclosing = _enclosing;
					this.serialVersionUID = 1L;
					this.gotOverflow = false;
				}

				private const long serialVersionUID;

				private bool gotOverflow;

				protected override bool RemoveEldestEntry(KeyValuePair<K2, V> e)
				{
					bool overflow = this._enclosing.ExpireKey2At(this._enclosing._enclosing._enclosing
						.Count);
					if (overflow && !this.gotOverflow)
					{
						Interns.Log.Warn("Metrics intern cache overflow at " + this._enclosing._enclosing
							._enclosing.Count + " for " + e);
						this.gotOverflow = true;
					}
					return overflow;
				}

				private readonly CacheWith2Keys<K1, K2, V> _enclosing;
			}

			public CacheWith2Keys()
			{
				k1Map = new _LinkedHashMap_43(this);
			}
		}

		internal const int MaxInfoNames = 2010;

		internal const int MaxInfoDescs = 100;

		[System.Serializable]
		internal sealed class Info
		{
			public static readonly Interns.Info Instance = new Interns.Info();

			private sealed class _CacheWith2Keys_95 : Interns.CacheWith2Keys<string, string, 
				MetricsInfo>
			{
				public _CacheWith2Keys_95()
				{
				}

				// Sanity limits in case of misuse/abuse.
				// distinct per name
				protected internal override bool ExpireKey1At(int size)
				{
					return size > Interns.MaxInfoNames;
				}

				protected internal override bool ExpireKey2At(int size)
				{
					return size > Interns.MaxInfoDescs;
				}

				protected internal override MetricsInfo NewValue(string name, string desc)
				{
					return new MetricsInfoImpl(name, desc);
				}
			}

			internal readonly Interns.CacheWith2Keys<string, string, MetricsInfo> cache = new 
				_CacheWith2Keys_95();
		}

		/// <summary>Get a metric info object</summary>
		/// <param name="name"/>
		/// <param name="description"/>
		/// <returns>an interned metric info object</returns>
		public static MetricsInfo Info(string name, string description)
		{
			return Interns.Info.Instance.cache.Add(name, description);
		}

		internal const int MaxTagNames = 100;

		internal const int MaxTagValues = 1000;

		[System.Serializable]
		internal sealed class Tags
		{
			public static readonly Interns.Tags Instance = new Interns.Tags();

			private sealed class _CacheWith2Keys_129 : Interns.CacheWith2Keys<MetricsInfo, string
				, MetricsTag>
			{
				public _CacheWith2Keys_129()
				{
				}

				// Sanity limits
				// distinct per name
				protected internal override bool ExpireKey1At(int size)
				{
					return size > Interns.MaxTagNames;
				}

				protected internal override bool ExpireKey2At(int size)
				{
					return size > Interns.MaxTagValues;
				}

				protected internal override MetricsTag NewValue(MetricsInfo info, string value)
				{
					return new MetricsTag(info, value);
				}
			}

			internal readonly Interns.CacheWith2Keys<MetricsInfo, string, MetricsTag> cache = 
				new _CacheWith2Keys_129();
		}

		/// <summary>Get a metrics tag</summary>
		/// <param name="info">of the tag</param>
		/// <param name="value">of the tag</param>
		/// <returns>an interned metrics tag</returns>
		public static MetricsTag Tag(MetricsInfo info, string value)
		{
			return Interns.Tags.Instance.cache.Add(info, value);
		}

		/// <summary>Get a metrics tag</summary>
		/// <param name="name">of the tag</param>
		/// <param name="description">of the tag</param>
		/// <param name="value">of the tag</param>
		/// <returns>an interned metrics tag</returns>
		public static MetricsTag Tag(string name, string description, string value)
		{
			return Interns.Tags.Instance.cache.Add(Info(name, description), value);
		}
	}
}
