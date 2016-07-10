using System;
using System.IO;

namespace Hadoop.Common.Core.IO
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
	public class ArrayWritable : IWritable
	{
		private Type valueClass;

		private IWritable[] values;

		public ArrayWritable(Type valueClass)
		{
			if (valueClass == null)
			{
				throw new ArgumentException("null valueClass");
			}
			this.valueClass = valueClass;
		}

		public ArrayWritable(Type valueClass, IWritable[] values)
			: this(valueClass)
		{
			this.values = values;
		}

		public ArrayWritable(string[] strings)
			: this(typeof(UTF8), new IWritable[strings.Length])
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
			var result = System.Array.CreateInstance(valueClass, values.Length);
            values.CopyTo(result, 0);            
			return result;
		}

		public virtual void Set(IWritable[] values)
		{
			this.values = values;
		}

		public virtual IWritable[] Get()
		{
			return values;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			values = new IWritable[reader.Read()];
			// construct values
			for (int i = 0; i < values.Length; i++)
			{
				IWritable value = WritableFactories.NewInstance(valueClass);
				value.ReadFields(reader);
				// read a value
				values[i] = value;
			}
		}

		// store it in values
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			writer.Write(values.Length);
			// write values
			for (int i = 0; i < values.Length; i++)
			{
				values[i].Write(writer);
			}
		}
	}
}
