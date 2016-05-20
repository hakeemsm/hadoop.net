using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A Writable SortedMap.</summary>
	public class SortedMapWritable : org.apache.hadoop.io.AbstractMapWritable, System.Collections.Generic.SortedDictionary
		<org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable>
	{
		private System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.WritableComparable
			, org.apache.hadoop.io.Writable> instance;

		/// <summary>default constructor.</summary>
		public SortedMapWritable()
			: base()
		{
			this.instance = new System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.WritableComparable
				, org.apache.hadoop.io.Writable>();
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="other">the map to copy from</param>
		public SortedMapWritable(org.apache.hadoop.io.SortedMapWritable other)
			: this()
		{
			copy(other);
		}

		public virtual java.util.Comparator<org.apache.hadoop.io.WritableComparable> comparator
			()
		{
			// Returning null means we use the natural ordering of the keys
			return null;
		}

		public virtual org.apache.hadoop.io.WritableComparable firstKey()
		{
			return instance.firstKey();
		}

		public virtual System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.WritableComparable
			, org.apache.hadoop.io.Writable> headMap(org.apache.hadoop.io.WritableComparable
			 toKey)
		{
			return instance.headMap(toKey);
		}

		public virtual org.apache.hadoop.io.WritableComparable lastKey()
		{
			return instance.lastKey();
		}

		public virtual System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.WritableComparable
			, org.apache.hadoop.io.Writable> subMap(org.apache.hadoop.io.WritableComparable 
			fromKey, org.apache.hadoop.io.WritableComparable toKey)
		{
			return instance.subMap(fromKey, toKey);
		}

		public virtual System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.WritableComparable
			, org.apache.hadoop.io.Writable> tailMap(org.apache.hadoop.io.WritableComparable
			 fromKey)
		{
			return instance.tailMap(fromKey);
		}

		public virtual void clear()
		{
			instance.clear();
		}

		public virtual bool Contains(object key)
		{
			return instance.Contains(key);
		}

		public virtual bool containsValue(object value)
		{
			return instance.containsValue(value);
		}

		public virtual System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair
			<org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.Writable>> entrySet
			()
		{
			return instance;
		}

		public virtual org.apache.hadoop.io.Writable get(object key)
		{
			return instance[key];
		}

		public virtual bool isEmpty()
		{
			return instance.isEmpty();
		}

		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.io.WritableComparable
			> Keys
		{
			get
			{
				return instance.Keys;
			}
		}

		public virtual org.apache.hadoop.io.Writable put(org.apache.hadoop.io.WritableComparable
			 key, org.apache.hadoop.io.Writable value)
		{
			addToMap(Sharpen.Runtime.getClassForObject(key));
			addToMap(Sharpen.Runtime.getClassForObject(value));
			return instance[key] = value;
		}

		public virtual void putAll<_T0>(System.Collections.Generic.IDictionary<_T0> t)
			where _T0 : org.apache.hadoop.io.WritableComparable
		{
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.WritableComparable
				, org.apache.hadoop.io.Writable> e in t)
			{
				this[e.Key] = e.Value;
			}
		}

		public virtual org.apache.hadoop.io.Writable remove(object key)
		{
			return Sharpen.Collections.Remove(instance, key);
		}

		public virtual int Count
		{
			get
			{
				return instance.Count;
			}
		}

		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.io.Writable
			> Values
		{
			get
			{
				return instance.Values;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void readFields(java.io.DataInput @in)
		{
			base.readFields(@in);
			// Read the number of entries in the map
			int entries = @in.readInt();
			// Then read each key/value pair
			for (int i = 0; i < entries; i++)
			{
				org.apache.hadoop.io.WritableComparable key = (org.apache.hadoop.io.WritableComparable
					)org.apache.hadoop.util.ReflectionUtils.newInstance(getClass(@in.readByte()), getConf
					());
				key.readFields(@in);
				org.apache.hadoop.io.Writable value = (org.apache.hadoop.io.Writable)org.apache.hadoop.util.ReflectionUtils
					.newInstance(getClass(@in.readByte()), getConf());
				value.readFields(@in);
				instance[key] = value;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(java.io.DataOutput @out)
		{
			base.write(@out);
			// Write out the number of entries in the map
			@out.writeInt(instance.Count);
			// Then write out each key/value pair
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.WritableComparable
				, org.apache.hadoop.io.Writable> e in instance)
			{
				@out.writeByte(getId(Sharpen.Runtime.getClassForObject(e.Key)));
				e.Key.write(@out);
				@out.writeByte(getId(Sharpen.Runtime.getClassForObject(e.Value)));
				e.Value.write(@out);
			}
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj is org.apache.hadoop.io.SortedMapWritable)
			{
				System.Collections.IDictionary map = (System.Collections.IDictionary)obj;
				if (Count != map.Count)
				{
					return false;
				}
				return this.Equals(map);
			}
			return false;
		}

		public override int GetHashCode()
		{
			return instance.GetHashCode();
		}
	}
}
