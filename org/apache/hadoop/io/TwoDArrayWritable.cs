using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A Writable for 2D arrays containing a matrix of instances of a class.</summary>
	public class TwoDArrayWritable : org.apache.hadoop.io.Writable
	{
		private java.lang.Class valueClass;

		private org.apache.hadoop.io.Writable[][] values;

		public TwoDArrayWritable(java.lang.Class valueClass)
		{
			this.valueClass = valueClass;
		}

		public TwoDArrayWritable(java.lang.Class valueClass, org.apache.hadoop.io.Writable
			[][] values)
			: this(valueClass)
		{
			this.values = values;
		}

		public virtual object toArray()
		{
			int[] dimensions = new int[] { values.Length, 0 };
			object result = java.lang.reflect.Array.newInstance(valueClass, dimensions);
			for (int i = 0; i < values.Length; i++)
			{
				object resultRow = java.lang.reflect.Array.newInstance(valueClass, values[i].Length
					);
				java.lang.reflect.Array.set(result, i, resultRow);
				for (int j = 0; j < values[i].Length; j++)
				{
					java.lang.reflect.Array.set(resultRow, j, values[i][j]);
				}
			}
			return result;
		}

		public virtual void set(org.apache.hadoop.io.Writable[][] values)
		{
			this.values = values;
		}

		public virtual org.apache.hadoop.io.Writable[][] get()
		{
			return values;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			// construct matrix
			values = new org.apache.hadoop.io.Writable[@in.readInt()][];
			for (int i = 0; i < values.Length; i++)
			{
				values[i] = new org.apache.hadoop.io.Writable[@in.readInt()];
			}
			// construct values
			for (int i_1 = 0; i_1 < values.Length; i_1++)
			{
				for (int j = 0; j < values[i_1].Length; j++)
				{
					org.apache.hadoop.io.Writable value;
					// construct value
					try
					{
						value = (org.apache.hadoop.io.Writable)valueClass.newInstance();
					}
					catch (java.lang.InstantiationException e)
					{
						throw new System.Exception(e.ToString());
					}
					catch (java.lang.IllegalAccessException e)
					{
						throw new System.Exception(e.ToString());
					}
					value.readFields(@in);
					// read a value
					values[i_1][j] = value;
				}
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
				@out.writeInt(values[i].Length);
			}
			for (int i_1 = 0; i_1 < values.Length; i_1++)
			{
				for (int j = 0; j < values[i_1].Length; j++)
				{
					values[i_1][j].write(@out);
				}
			}
		}
	}
}
