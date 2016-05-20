using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>Helpers to create interned metrics info</summary>
	public class Interns
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.Interns
			)));

		private abstract class CacheWith2Keys<K1, K2, V>
		{
			private sealed class _LinkedHashMap_43 : java.util.LinkedHashMap<K1, System.Collections.Generic.IDictionary
				<K2, V>>
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
				protected override bool removeEldestEntry(System.Collections.Generic.KeyValuePair
					<K1, System.Collections.Generic.IDictionary<K2, V>> e)
				{
					bool overflow = this._enclosing.expireKey1At(this._enclosing._enclosing._enclosing
						.Count);
					if (overflow && !this.gotOverflow)
					{
						org.apache.hadoop.metrics2.lib.Interns.LOG.warn("Metrics intern cache overflow at "
							 + this._enclosing._enclosing._enclosing.Count + " for " + e);
						this.gotOverflow = true;
					}
					return overflow;
				}

				private readonly CacheWith2Keys<K1, K2, V> _enclosing;
			}

			private readonly System.Collections.Generic.IDictionary<K1, System.Collections.Generic.IDictionary
				<K2, V>> k1Map;

			protected internal abstract bool expireKey1At(int size);

			protected internal abstract bool expireKey2At(int size);

			protected internal abstract V newValue(K1 k1, K2 k2);

			internal virtual V add(K1 k1, K2 k2)
			{
				lock (this)
				{
					System.Collections.Generic.IDictionary<K2, V> k2Map = k1Map[k1];
					if (k2Map == null)
					{
						k2Map = new _LinkedHashMap_64(this);
						k1Map[k1] = k2Map;
					}
					V v = k2Map[k2];
					if (v == null)
					{
						v = newValue(k1, k2);
						k2Map[k2] = v;
					}
					return v;
				}
			}

			private sealed class _LinkedHashMap_64 : java.util.LinkedHashMap<K2, V>
			{
				public _LinkedHashMap_64(CacheWith2Keys<K1, K2, V> _enclosing)
				{
					this._enclosing = _enclosing;
					this.serialVersionUID = 1L;
					this.gotOverflow = false;
				}

				private const long serialVersionUID;

				private bool gotOverflow;

				protected override bool removeEldestEntry(System.Collections.Generic.KeyValuePair
					<K2, V> e)
				{
					bool overflow = this._enclosing.expireKey2At(this._enclosing._enclosing._enclosing
						.Count);
					if (overflow && !this.gotOverflow)
					{
						org.apache.hadoop.metrics2.lib.Interns.LOG.warn("Metrics intern cache overflow at "
							 + this._enclosing._enclosing._enclosing.Count + " for " + e);
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

		internal const int MAX_INFO_NAMES = 2010;

		internal const int MAX_INFO_DESCS = 100;

		[System.Serializable]
		internal sealed class Info
		{
			public static readonly org.apache.hadoop.metrics2.lib.Interns.Info INSTANCE = new 
				org.apache.hadoop.metrics2.lib.Interns.Info();

			private sealed class _CacheWith2Keys_95 : org.apache.hadoop.metrics2.lib.Interns.CacheWith2Keys
				<string, string, org.apache.hadoop.metrics2.MetricsInfo>
			{
				public _CacheWith2Keys_95()
				{
				}

				// Sanity limits in case of misuse/abuse.
				// distinct per name
				protected internal override bool expireKey1At(int size)
				{
					return size > org.apache.hadoop.metrics2.lib.Interns.MAX_INFO_NAMES;
				}

				protected internal override bool expireKey2At(int size)
				{
					return size > org.apache.hadoop.metrics2.lib.Interns.MAX_INFO_DESCS;
				}

				protected internal override org.apache.hadoop.metrics2.MetricsInfo newValue(string
					 name, string desc)
				{
					return new org.apache.hadoop.metrics2.lib.MetricsInfoImpl(name, desc);
				}
			}

			internal readonly org.apache.hadoop.metrics2.lib.Interns.CacheWith2Keys<string, string
				, org.apache.hadoop.metrics2.MetricsInfo> cache = new _CacheWith2Keys_95();
		}

		/// <summary>Get a metric info object</summary>
		/// <param name="name"/>
		/// <param name="description"/>
		/// <returns>an interned metric info object</returns>
		public static org.apache.hadoop.metrics2.MetricsInfo info(string name, string description
			)
		{
			return org.apache.hadoop.metrics2.lib.Interns.Info.INSTANCE.cache.add(name, description
				);
		}

		internal const int MAX_TAG_NAMES = 100;

		internal const int MAX_TAG_VALUES = 1000;

		[System.Serializable]
		internal sealed class Tags
		{
			public static readonly org.apache.hadoop.metrics2.lib.Interns.Tags INSTANCE = new 
				org.apache.hadoop.metrics2.lib.Interns.Tags();

			private sealed class _CacheWith2Keys_129 : org.apache.hadoop.metrics2.lib.Interns.CacheWith2Keys
				<org.apache.hadoop.metrics2.MetricsInfo, string, org.apache.hadoop.metrics2.MetricsTag
				>
			{
				public _CacheWith2Keys_129()
				{
				}

				// Sanity limits
				// distinct per name
				protected internal override bool expireKey1At(int size)
				{
					return size > org.apache.hadoop.metrics2.lib.Interns.MAX_TAG_NAMES;
				}

				protected internal override bool expireKey2At(int size)
				{
					return size > org.apache.hadoop.metrics2.lib.Interns.MAX_TAG_VALUES;
				}

				protected internal override org.apache.hadoop.metrics2.MetricsTag newValue(org.apache.hadoop.metrics2.MetricsInfo
					 info, string value)
				{
					return new org.apache.hadoop.metrics2.MetricsTag(info, value);
				}
			}

			internal readonly org.apache.hadoop.metrics2.lib.Interns.CacheWith2Keys<org.apache.hadoop.metrics2.MetricsInfo
				, string, org.apache.hadoop.metrics2.MetricsTag> cache = new _CacheWith2Keys_129
				();
		}

		/// <summary>Get a metrics tag</summary>
		/// <param name="info">of the tag</param>
		/// <param name="value">of the tag</param>
		/// <returns>an interned metrics tag</returns>
		public static org.apache.hadoop.metrics2.MetricsTag tag(org.apache.hadoop.metrics2.MetricsInfo
			 info, string value)
		{
			return org.apache.hadoop.metrics2.lib.Interns.Tags.INSTANCE.cache.add(info, value
				);
		}

		/// <summary>Get a metrics tag</summary>
		/// <param name="name">of the tag</param>
		/// <param name="description">of the tag</param>
		/// <param name="value">of the tag</param>
		/// <returns>an interned metrics tag</returns>
		public static org.apache.hadoop.metrics2.MetricsTag tag(string name, string description
			, string value)
		{
			return org.apache.hadoop.metrics2.lib.Interns.Tags.INSTANCE.cache.add(info(name, 
				description), value);
		}
	}
}
