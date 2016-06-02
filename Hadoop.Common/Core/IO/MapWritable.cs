using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A Writable Map.</summary>
	public class MapWritable : AbstractMapWritable, IDictionary<Writable, Writable>
	{
		private IDictionary<Writable, Writable> instance;

		/// <summary>Default constructor.</summary>
		public MapWritable()
			: base()
		{
			this.instance = new Dictionary<Writable, Writable>();
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="other">the map to copy from</param>
		public MapWritable(Org.Apache.Hadoop.IO.MapWritable other)
			: this()
		{
			Copy(other);
		}

		public virtual void Clear()
		{
			instance.Clear();
		}

		public virtual bool Contains(object key)
		{
			return instance.Contains(key);
		}

		public virtual bool ContainsValue(object value)
		{
			return instance.ContainsValue(value);
		}

		public virtual ICollection<KeyValuePair<Writable, Writable>> EntrySet()
		{
			return instance;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj is Org.Apache.Hadoop.IO.MapWritable)
			{
				Org.Apache.Hadoop.IO.MapWritable map = (Org.Apache.Hadoop.IO.MapWritable)obj;
				if (Count != map.Count)
				{
					return false;
				}
				return this.Equals(map);
			}
			return false;
		}

		public virtual Writable Get(object key)
		{
			return instance[key];
		}

		public override int GetHashCode()
		{
			return 1 + this.instance.GetHashCode();
		}

		public virtual bool IsEmpty()
		{
			return instance.IsEmpty();
		}

		public virtual ICollection<Writable> Keys
		{
			get
			{
				return instance.Keys;
			}
		}

		public virtual Writable Put(Writable key, Writable value)
		{
			AddToMap(key.GetType());
			AddToMap(value.GetType());
			return instance[key] = value;
		}

		public virtual void PutAll<_T0>(IDictionary<_T0> t)
			where _T0 : Writable
		{
			foreach (KeyValuePair<Writable, Writable> e in t)
			{
				this[e.Key] = e.Value;
			}
		}

		public virtual Writable Remove(object key)
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

		public virtual ICollection<Writable> Values
		{
			get
			{
				return instance.Values;
			}
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			// Write out the number of entries in the map
			@out.WriteInt(instance.Count);
			// Then write out each key/value pair
			foreach (KeyValuePair<Writable, Writable> e in instance)
			{
				@out.WriteByte(GetId(e.Key.GetType()));
				e.Key.Write(@out);
				@out.WriteByte(GetId(e.Value.GetType()));
				e.Value.Write(@out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			// First clear the map.  Otherwise we will just accumulate
			// entries every time this method is called.
			this.instance.Clear();
			// Read the number of entries in the map
			int entries = @in.ReadInt();
			// Then read each key/value pair
			for (int i = 0; i < entries; i++)
			{
				Writable key = (Writable)ReflectionUtils.NewInstance(GetClass(@in.ReadByte()), GetConf
					());
				key.ReadFields(@in);
				Writable value = (Writable)ReflectionUtils.NewInstance(GetClass(@in.ReadByte()), 
					GetConf());
				value.ReadFields(@in);
				instance[key] = value;
			}
		}
	}
}
