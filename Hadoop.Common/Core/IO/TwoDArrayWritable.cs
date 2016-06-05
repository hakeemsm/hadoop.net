using System;
using System.IO;
using Hadoop.Common.Core.IO;

using Reflect;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A Writable for 2D arrays containing a matrix of instances of a class.</summary>
	public class TwoDArrayWritable : IWritable
	{
		private Type valueClass;

		private IWritable[][] values;

		public TwoDArrayWritable(Type valueClass)
		{
			this.valueClass = valueClass;
		}

		public TwoDArrayWritable(Type valueClass, IWritable[][] values)
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
				Runtime.SetArrayValue(result, i, resultRow);
				for (int j = 0; j < values[i].Length; j++)
				{
					Runtime.SetArrayValue(resultRow, j, values[i][j]);
				}
			}
			return result;
		}

		public virtual void Set(IWritable[][] values)
		{
			this.values = values;
		}

		public virtual IWritable[][] Get()
		{
			return values;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			// construct matrix
			values = new IWritable[@in.ReadInt()][];
			for (int i = 0; i < values.Length; i++)
			{
				values[i] = new IWritable[@in.ReadInt()];
			}
			// construct values
			for (int i_1 = 0; i_1 < values.Length; i_1++)
			{
				for (int j = 0; j < values[i_1].Length; j++)
				{
					IWritable value;
					// construct value
					try
					{
						value = (IWritable)System.Activator.CreateInstance(valueClass);
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
		public virtual void Write(BinaryWriter @out)
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
