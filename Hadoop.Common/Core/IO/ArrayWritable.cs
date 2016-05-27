using System;
using System.IO;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A Writable for arrays containing instances of a class.</summary>
	/// <remarks>
	/// A Writable for arrays containing instances of a class. The elements of this
	/// writable must all be instances of the same class. If this writable will be
	/// the input for a Reducer, you will need to create a subclass that sets the
	/// value to be of the proper type.
	/// For example:
	/// <code>
	/// public class IntArrayWritable extends ArrayWritable {
	/// public IntArrayWritable() {
	/// super(IntWritable.class);
	/// }
	/// }
	/// </code>
	/// </remarks>
	public class ArrayWritable : Writable
	{
		private Type valueClass;

		private Writable[] values;

		public ArrayWritable(Type valueClass)
		{
			if (valueClass == null)
			{
				throw new ArgumentException("null valueClass");
			}
			this.valueClass = valueClass;
		}

		public ArrayWritable(Type valueClass, Writable[] values)
			: this(valueClass)
		{
			this.values = values;
		}

		public ArrayWritable(string[] strings)
			: this(typeof(UTF8), new Writable[strings.Length])
		{
			for (int i = 0; i < strings.Length; i++)
			{
				values[i] = new UTF8(strings[i]);
			}
		}

		public virtual Type GetValueClass()
		{
			return valueClass;
		}

		public virtual string[] ToStrings()
		{
			string[] strings = new string[values.Length];
			for (int i = 0; i < values.Length; i++)
			{
				strings[i] = values[i].ToString();
			}
			return strings;
		}

		public virtual object ToArray()
		{
			object result = System.Array.CreateInstance(valueClass, values.Length);
			for (int i = 0; i < values.Length; i++)
			{
				Sharpen.Runtime.SetArrayValue(result, i, values[i]);
			}
			return result;
		}

		public virtual void Set(Writable[] values)
		{
			this.values = values;
		}

		public virtual Writable[] Get()
		{
			return values;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			values = new Writable[@in.ReadInt()];
			// construct values
			for (int i = 0; i < values.Length; i++)
			{
				Writable value = WritableFactories.NewInstance(valueClass);
				value.ReadFields(@in);
				// read a value
				values[i] = value;
			}
		}

		// store it in values
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteInt(values.Length);
			// write values
			for (int i = 0; i < values.Length; i++)
			{
				values[i].Write(@out);
			}
		}
	}
}
