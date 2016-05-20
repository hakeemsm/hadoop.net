using Sharpen;

namespace org.apache.hadoop.io
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
	public class ArrayWritable : org.apache.hadoop.io.Writable
	{
		private java.lang.Class valueClass;

		private org.apache.hadoop.io.Writable[] values;

		public ArrayWritable(java.lang.Class valueClass)
		{
			if (valueClass == null)
			{
				throw new System.ArgumentException("null valueClass");
			}
			this.valueClass = valueClass;
		}

		public ArrayWritable(java.lang.Class valueClass, org.apache.hadoop.io.Writable[] 
			values)
			: this(valueClass)
		{
			this.values = values;
		}

		public ArrayWritable(string[] strings)
			: this(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.UTF8)), new org.apache.hadoop.io.Writable
				[strings.Length])
		{
			for (int i = 0; i < strings.Length; i++)
			{
				values[i] = new org.apache.hadoop.io.UTF8(strings[i]);
			}
		}

		public virtual java.lang.Class getValueClass()
		{
			return valueClass;
		}

		public virtual string[] toStrings()
		{
			string[] strings = new string[values.Length];
			for (int i = 0; i < values.Length; i++)
			{
				strings[i] = values[i].ToString();
			}
			return strings;
		}

		public virtual object toArray()
		{
			object result = java.lang.reflect.Array.newInstance(valueClass, values.Length);
			for (int i = 0; i < values.Length; i++)
			{
				java.lang.reflect.Array.set(result, i, values[i]);
			}
			return result;
		}

		public virtual void set(org.apache.hadoop.io.Writable[] values)
		{
			this.values = values;
		}

		public virtual org.apache.hadoop.io.Writable[] get()
		{
			return values;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			values = new org.apache.hadoop.io.Writable[@in.readInt()];
			// construct values
			for (int i = 0; i < values.Length; i++)
			{
				org.apache.hadoop.io.Writable value = org.apache.hadoop.io.WritableFactories.newInstance
					(valueClass);
				value.readFields(@in);
				// read a value
				values[i] = value;
			}
		}

		// store it in values
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeInt(values.Length);
			// write values
			for (int i = 0; i < values.Length; i++)
			{
				values[i].write(@out);
			}
		}
	}
}
