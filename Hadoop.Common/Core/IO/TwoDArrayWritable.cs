using System;
using System.IO;
using Hadoop.Common.Core.IO;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A Writable for 2D arrays containing a matrix of instances of a class.</summary>
	public class TwoDArrayWritable : Writable
	{
		private Type valueClass;

		private Writable[][] values;

		public TwoDArrayWritable(Type valueClass)
		{
			this.valueClass = valueClass;
		}

		public TwoDArrayWritable(Type valueClass, Writable[][] values)
			: this(valueClass)
		{
			this.values = values;
		}

		public virtual object ToArray()
		{
			int[] dimensions = new int[] { values.Length, 0 };
			object result = System.Array.CreateInstance(valueClass, dimensions);
			for (int i = 0; i < values.Length; i++)
			{
				object resultRow = System.Array.CreateInstance(valueClass, values[i].Length);
				Sharpen.Runtime.SetArrayValue(result, i, resultRow);
				for (int j = 0; j < values[i].Length; j++)
				{
					Sharpen.Runtime.SetArrayValue(resultRow, j, values[i][j]);
				}
			}
			return result;
		}

		public virtual void Set(Writable[][] values)
		{
			this.values = values;
		}

		public virtual Writable[][] Get()
		{
			return values;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			// construct matrix
			values = new Writable[@in.ReadInt()][];
			for (int i = 0; i < values.Length; i++)
			{
				values[i] = new Writable[@in.ReadInt()];
			}
			// construct values
			for (int i_1 = 0; i_1 < values.Length; i_1++)
			{
				for (int j = 0; j < values[i_1].Length; j++)
				{
					Writable value;
					// construct value
					try
					{
						value = (Writable)System.Activator.CreateInstance(valueClass);
					}
					catch (InstantiationException e)
					{
						throw new RuntimeException(e.ToString());
					}
					catch (MemberAccessException e)
					{
						throw new RuntimeException(e.ToString());
					}
					value.ReadFields(@in);
					// read a value
					values[i_1][j] = value;
				}
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
				@out.WriteInt(values[i].Length);
			}
			for (int i_1 = 0; i_1 < values.Length; i_1++)
			{
				for (int j = 0; j < values[i_1].Length; j++)
				{
					values[i_1][j].Write(@out);
				}
			}
		}
	}
}
