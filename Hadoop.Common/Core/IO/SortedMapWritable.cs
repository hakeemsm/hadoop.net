using System.Collections;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A Writable SortedMap.</summary>
	public class SortedMapWritable : AbstractMapWritable, SortedDictionary<WritableComparable
		, Writable>
	{
		private SortedDictionary<WritableComparable, Writable> instance;

		/// <summary>default constructor.</summary>
		public SortedMapWritable()
			: base()
		{
			this.instance = new SortedDictionary<WritableComparable, Writable>();
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="other">the map to copy from</param>
		public SortedMapWritable(Org.Apache.Hadoop.IO.SortedMapWritable other)
			: this()
		{
			Copy(other);
		}

		public virtual IComparer<WritableComparable> Comparator()
		{
			// Returning null means we use the natural ordering of the keys
			return null;
		}

		public virtual WritableComparable FirstKey()
		{
			return instance.FirstKey();
		}

		public virtual SortedDictionary<WritableComparable, Writable> HeadMap(WritableComparable
			 toKey)
		{
			return instance.HeadMap(toKey);
		}

		public virtual WritableComparable LastKey()
		{
			return instance.LastKey();
		}

		public virtual SortedDictionary<WritableComparable, Writable> SubMap(WritableComparable
			 fromKey, WritableComparable toKey)
		{
			return instance.SubMap(fromKey, toKey);
		}

		public virtual SortedDictionary<WritableComparable, Writable> TailMap(WritableComparable
			 fromKey)
		{
			return instance.TailMap(fromKey);
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

		public virtual ICollection<KeyValuePair<WritableComparable, Writable>> EntrySet()
		{
			return instance;
		}

		public virtual Writable Get(object key)
		{
			return instance[key];
		}

		public virtual bool IsEmpty()
		{
			return instance.IsEmpty();
		}

		public virtual ICollection<WritableComparable> Keys
		{
			get
			{
				return instance.Keys;
			}
		}

		public virtual Writable Put(WritableComparable key, Writable value)
		{
			AddToMap(key.GetType());
			AddToMap(value.GetType());
			return instance[key] = value;
		}

		public virtual void PutAll<_T0>(IDictionary<_T0> t)
			where _T0 : WritableComparable
		{
			foreach (KeyValuePair<WritableComparable, Writable> e in t)
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

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			// Read the number of entries in the map
			int entries = @in.ReadInt();
			// Then read each key/value pair
			for (int i = 0; i < entries; i++)
			{
				WritableComparable key = (WritableComparable)ReflectionUtils.NewInstance(GetClass
					(@in.ReadByte()), GetConf());
				key.ReadFields(@in);
				Writable value = (Writable)ReflectionUtils.NewInstance(GetClass(@in.ReadByte()), 
					GetConf());
				value.ReadFields(@in);
				instance[key] = value;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			// Write out the number of entries in the map
			@out.WriteInt(instance.Count);
			// Then write out each key/value pair
			foreach (KeyValuePair<WritableComparable, Writable> e in instance)
			{
				@out.WriteByte(GetId(e.Key.GetType()));
				e.Key.Write(@out);
				@out.WriteByte(GetId(e.Value.GetType()));
				e.Value.Write(@out);
			}
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj is Org.Apache.Hadoop.IO.SortedMapWritable)
			{
				IDictionary map = (IDictionary)obj;
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
