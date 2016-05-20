using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A Writable Map.</summary>
	public class MapWritable : org.apache.hadoop.io.AbstractMapWritable, System.Collections.Generic.IDictionary
		<org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable>
	{
		private System.Collections.Generic.IDictionary<org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable
			> instance;

		/// <summary>Default constructor.</summary>
		public MapWritable()
			: base()
		{
			this.instance = new System.Collections.Generic.Dictionary<org.apache.hadoop.io.Writable
				, org.apache.hadoop.io.Writable>();
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="other">the map to copy from</param>
		public MapWritable(org.apache.hadoop.io.MapWritable other)
			: this()
		{
			copy(other);
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
			<org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable>> entrySet()
		{
			return instance;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj is org.apache.hadoop.io.MapWritable)
			{
				org.apache.hadoop.io.MapWritable map = (org.apache.hadoop.io.MapWritable)obj;
				if (Count != map.Count)
				{
					return false;
				}
				return this.Equals(map);
			}
			return false;
		}

		public virtual org.apache.hadoop.io.Writable get(object key)
		{
			return instance[key];
		}

		public override int GetHashCode()
		{
			return 1 + this.instance.GetHashCode();
		}

		public virtual bool isEmpty()
		{
			return instance.isEmpty();
		}

		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.io.Writable
			> Keys
		{
			get
			{
				return instance.Keys;
			}
		}

		public virtual org.apache.hadoop.io.Writable put(org.apache.hadoop.io.Writable key
			, org.apache.hadoop.io.Writable value)
		{
			addToMap(Sharpen.Runtime.getClassForObject(key));
			addToMap(Sharpen.Runtime.getClassForObject(value));
			return instance[key] = value;
		}

		public virtual void putAll<_T0>(System.Collections.Generic.IDictionary<_T0> t)
			where _T0 : org.apache.hadoop.io.Writable
		{
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable
				> e in t)
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

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public override void write(java.io.DataOutput @out)
		{
			base.write(@out);
			// Write out the number of entries in the map
			@out.writeInt(instance.Count);
			// Then write out each key/value pair
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable
				> e in instance)
			{
				@out.writeByte(getId(Sharpen.Runtime.getClassForObject(e.Key)));
				e.Key.write(@out);
				@out.writeByte(getId(Sharpen.Runtime.getClassForObject(e.Value)));
				e.Value.write(@out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void readFields(java.io.DataInput @in)
		{
			base.readFields(@in);
			// First clear the map.  Otherwise we will just accumulate
			// entries every time this method is called.
			this.instance.clear();
			// Read the number of entries in the map
			int entries = @in.readInt();
			// Then read each key/value pair
			for (int i = 0; i < entries; i++)
			{
				org.apache.hadoop.io.Writable key = (org.apache.hadoop.io.Writable)org.apache.hadoop.util.ReflectionUtils
					.newInstance(getClass(@in.readByte()), getConf());
				key.readFields(@in);
				org.apache.hadoop.io.Writable value = (org.apache.hadoop.io.Writable)org.apache.hadoop.util.ReflectionUtils
					.newInstance(getClass(@in.readByte()), getConf());
				value.readFields(@in);
				instance[key] = value;
			}
		}
	}
}
