using System.Collections;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A Writable SortedMap.</summary>
	public class SortedMapWritable : AbstractMapWritable, SortedDictionary<IWritableComparable<>, IWritable>
	{
		private SortedDictionary<IWritableComparable<>, IWritable> instance;

		/// <summary>default constructor.</summary>
		public SortedMapWritable()
			: base()
		{
			this.instance = new SortedDictionary<IWritableComparable<>, IWritable>();
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="other">the map to copy from</param>
		public SortedMapWritable(Org.Apache.Hadoop.IO.SortedMapWritable other)
			: this()
		{
			Copy(other);
		}

		public virtual IComparer<IWritableComparable<>> Comparator()
		{
			// Returning null means we use the natural ordering of the keys
			return null;
		}

		public virtual IWritableComparable<> FirstKey()
		{
			return instance.FirstKey();
		}

		public virtual SortedDictionary<IWritableComparable<>, IWritable> HeadMap(IWritableComparable<> toKey)
		{
			return instance.HeadMap(toKey);
		}

		public virtual IWritableComparable<> LastKey()
		{
			return instance.LastKey();
		}

		public virtual SortedDictionary<IWritableComparable<>, IWritable> SubMap(IWritableComparable<> fromKey, IWritableComparable<> toKey)
		{
			return instance.SubMap(fromKey, toKey);
		}

		public virtual SortedDictionary<IWritableComparable<>, IWritable> TailMap(IWritableComparable<> fromKey)
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

		public virtual ICollection<KeyValuePair<IWritableComparable<>, IWritable>> EntrySet()
		{
			return instance;
		}

		public virtual IWritable Get(object key)
		{
			return instance[key];
		}

		public virtual bool IsEmpty()
		{
			return instance.IsEmpty();
		}

		public virtual ICollection<IWritableComparable<>> Keys
		{
			get
			{
				return instance.Keys;
			}
		}

		public virtual IWritable Put(IWritableComparable<> key, IWritable value)
		{
			AddToMap(key.GetType());
			AddToMap(value.GetType());
			return instance[key] = value;
		}

		public virtual void PutAll<_T0>(IDictionary<_T0> t)
			where _T0 : IWritableComparable<>
		{
			foreach (KeyValuePair<IWritableComparable<>, IWritable> e in t)
			{
				this[e.Key] = e.Value;
			}
		}

		public virtual IWritable Remove(object key)
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

		public virtual ICollection<IWritable> Values
		{
			get
			{
				return instance.Values;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(BinaryReader @in)
		{
			base.ReadFields(@in);
			// Read the number of entries in the map
			int entries = @in.ReadInt();
			// Then read each key/value pair
			for (int i = 0; i < entries; i++)
			{
				IWritableComparable<> key = (IWritableComparable<>)ReflectionUtils.NewInstance(GetClass
					(@in.ReadByte()), GetConf());
				key.ReadFields(@in);
				IWritable value = (IWritable)ReflectionUtils.NewInstance(GetClass(@in.ReadByte()), 
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
			foreach (KeyValuePair<IWritableComparable<>, IWritable> e in instance)
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
