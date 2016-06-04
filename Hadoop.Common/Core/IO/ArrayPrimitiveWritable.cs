using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>This is a wrapper class.</summary>
	/// <remarks>
	/// This is a wrapper class.  It wraps a Writable implementation around
	/// an array of primitives (e.g., int[], long[], etc.), with optimized
	/// wire format, and without creating new objects per element.
	/// This is a wrapper class only; it does not make a copy of the
	/// underlying array.
	/// </remarks>
	public class ArrayPrimitiveWritable : IWritable
	{
		private Type componentType = null;

		private Type declaredComponentType = null;

		private int length;

		private object value;

		private static readonly IDictionary<string, Type> PrimitiveNames = new Dictionary
			<string, Type>(16);

		static ArrayPrimitiveWritable()
		{
			//componentType is determined from the component type of the value array 
			//during a "set" operation.  It must be primitive.
			//declaredComponentType need not be declared, but if you do (by using the
			//ArrayPrimitiveWritable(Class<?>) constructor), it will provide typechecking
			//for all "set" operations.
			//must be an array of <componentType>[length]
			PrimitiveNames[typeof(bool).FullName] = typeof(bool);
			PrimitiveNames[typeof(byte).FullName] = typeof(byte);
			PrimitiveNames[typeof(char).FullName] = typeof(char);
			PrimitiveNames[typeof(short).FullName] = typeof(short);
			PrimitiveNames[typeof(int).FullName] = typeof(int);
			PrimitiveNames[typeof(long).FullName] = typeof(long);
			PrimitiveNames[typeof(float).FullName] = typeof(float);
			PrimitiveNames[typeof(double).FullName] = typeof(double);
		}

		private static Type GetPrimitiveClass(string className)
		{
			return PrimitiveNames[className];
		}

		private static void CheckPrimitive(Type componentType)
		{
			if (componentType == null)
			{
				throw new HadoopIllegalArgumentException("null component type not allowed");
			}
			if (!PrimitiveNames.Contains(componentType.FullName))
			{
				throw new HadoopIllegalArgumentException("input array component type " + componentType
					.FullName + " is not a candidate primitive type");
			}
		}

		private void CheckDeclaredComponentType(Type componentType)
		{
			if ((declaredComponentType != null) && (componentType != declaredComponentType))
			{
				throw new HadoopIllegalArgumentException("input array component type " + componentType
					.FullName + " does not match declared type " + declaredComponentType.FullName);
			}
		}

		private static void CheckArray(object value)
		{
			if (value == null)
			{
				throw new HadoopIllegalArgumentException("null value not allowed");
			}
			if (!value.GetType().IsArray)
			{
				throw new HadoopIllegalArgumentException("non-array value of class " + value.GetType
					() + " not allowed");
			}
		}

		/// <summary>Construct an empty instance, for use during Writable read</summary>
		public ArrayPrimitiveWritable()
		{
		}

		/// <summary>
		/// Construct an instance of known type but no value yet
		/// for use with type-specific wrapper classes
		/// </summary>
		public ArrayPrimitiveWritable(Type componentType)
		{
			//empty constructor
			CheckPrimitive(componentType);
			this.declaredComponentType = componentType;
		}

		/// <summary>Wrap an existing array of primitives</summary>
		/// <param name="value">- array of primitives</param>
		public ArrayPrimitiveWritable(object value)
		{
			Set(value);
		}

		/// <summary>Get the original array.</summary>
		/// <remarks>
		/// Get the original array.
		/// Client must cast it back to type componentType[]
		/// (or may use type-specific wrapper classes).
		/// </remarks>
		/// <returns>- original array as Object</returns>
		public virtual object Get()
		{
			return value;
		}

		public virtual Type GetComponentType()
		{
			return componentType;
		}

		public virtual Type GetDeclaredComponentType()
		{
			return declaredComponentType;
		}

		public virtual bool IsDeclaredComponentType(Type componentType)
		{
			return componentType == declaredComponentType;
		}

		public virtual void Set(object value)
		{
			CheckArray(value);
			Type componentType = value.GetType().GetElementType();
			CheckPrimitive(componentType);
			CheckDeclaredComponentType(componentType);
			this.componentType = componentType;
			this.value = value;
			this.length = Sharpen.Runtime.GetArrayLength(value);
		}

		/// <summary>Do not use this class.</summary>
		/// <remarks>
		/// Do not use this class.
		/// This is an internal class, purely for ObjectWritable to use as
		/// a label class for transparent conversions of arrays of primitives
		/// during wire protocol reads and writes.
		/// </remarks>
		internal class Internal : ArrayPrimitiveWritable
		{
			internal Internal()
				: base()
			{
			}

			internal Internal(object value)
				: base(value)
			{
			}
			//use for reads
			//use for writes
		}

		//end Internal subclass declaration
		/*
		* @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			// write componentType 
			UTF8.WriteString(@out, componentType.FullName);
			// write length
			@out.WriteInt(length);
			// do the inner loop.  Walk the decision tree only once.
			if (componentType == typeof(bool))
			{
				// boolean
				WriteBooleanArray(@out);
			}
			else
			{
				if (componentType == typeof(char))
				{
					// char
					WriteCharArray(@out);
				}
				else
				{
					if (componentType == typeof(byte))
					{
						// byte
						WriteByteArray(@out);
					}
					else
					{
						if (componentType == typeof(short))
						{
							// short
							WriteShortArray(@out);
						}
						else
						{
							if (componentType == typeof(int))
							{
								// int
								WriteIntArray(@out);
							}
							else
							{
								if (componentType == typeof(long))
								{
									// long
									WriteLongArray(@out);
								}
								else
								{
									if (componentType == typeof(float))
									{
										// float
										WriteFloatArray(@out);
									}
									else
									{
										if (componentType == typeof(double))
										{
											// double
											WriteDoubleArray(@out);
										}
										else
										{
											throw new IOException("Component type " + componentType.ToString() + " is set as the output type, but no encoding is implemented for this type."
												);
										}
									}
								}
							}
						}
					}
				}
			}
		}

		/*
		* @see org.apache.hadoop.io.Writable#readFields(java.io.BinaryReader)
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			// read and set the component type of the array
			string className = UTF8.ReadString(@in);
			Type componentType = GetPrimitiveClass(className);
			if (componentType == null)
			{
				throw new IOException("encoded array component type " + className + " is not a candidate primitive type"
					);
			}
			CheckDeclaredComponentType(componentType);
			this.componentType = componentType;
			// read and set the length of the array
			int length = @in.ReadInt();
			if (length < 0)
			{
				throw new IOException("encoded array length is negative " + length);
			}
			this.length = length;
			// construct and read in the array
			value = System.Array.CreateInstance(componentType, length);
			// do the inner loop.  Walk the decision tree only once.
			if (componentType == typeof(bool))
			{
				// boolean
				ReadBooleanArray(@in);
			}
			else
			{
				if (componentType == typeof(char))
				{
					// char
					ReadCharArray(@in);
				}
				else
				{
					if (componentType == typeof(byte))
					{
						// byte
						ReadByteArray(@in);
					}
					else
					{
						if (componentType == typeof(short))
						{
							// short
							ReadShortArray(@in);
						}
						else
						{
							if (componentType == typeof(int))
							{
								// int
								ReadIntArray(@in);
							}
							else
							{
								if (componentType == typeof(long))
								{
									// long
									ReadLongArray(@in);
								}
								else
								{
									if (componentType == typeof(float))
									{
										// float
										ReadFloatArray(@in);
									}
									else
									{
										if (componentType == typeof(double))
										{
											// double
											ReadDoubleArray(@in);
										}
										else
										{
											throw new IOException("Encoded type " + className + " converted to valid component type "
												 + componentType.ToString() + " but no encoding is implemented for this type.");
										}
									}
								}
							}
						}
					}
				}
			}
		}

		//For efficient implementation, there's no way around
		//the following massive code duplication.
		/// <exception cref="System.IO.IOException"/>
		private void WriteBooleanArray(DataOutput @out)
		{
			bool[] v = (bool[])value;
			for (int i = 0; i < length; i++)
			{
				@out.WriteBoolean(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteCharArray(DataOutput @out)
		{
			char[] v = (char[])value;
			for (int i = 0; i < length; i++)
			{
				@out.WriteChar(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteByteArray(DataOutput @out)
		{
			@out.Write((byte[])value, 0, length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteShortArray(DataOutput @out)
		{
			short[] v = (short[])value;
			for (int i = 0; i < length; i++)
			{
				@out.WriteShort(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteIntArray(DataOutput @out)
		{
			int[] v = (int[])value;
			for (int i = 0; i < length; i++)
			{
				@out.WriteInt(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteLongArray(DataOutput @out)
		{
			long[] v = (long[])value;
			for (int i = 0; i < length; i++)
			{
				@out.WriteLong(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFloatArray(DataOutput @out)
		{
			float[] v = (float[])value;
			for (int i = 0; i < length; i++)
			{
				@out.WriteFloat(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteDoubleArray(DataOutput @out)
		{
			double[] v = (double[])value;
			for (int i = 0; i < length; i++)
			{
				@out.WriteDouble(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadBooleanArray(BinaryReader @in)
		{
			bool[] v = (bool[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.ReadBoolean();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadCharArray(BinaryReader @in)
		{
			char[] v = (char[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.ReadChar();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadByteArray(BinaryReader @in)
		{
			@in.ReadFully((byte[])value, 0, length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadShortArray(BinaryReader @in)
		{
			short[] v = (short[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.ReadShort();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadIntArray(BinaryReader @in)
		{
			int[] v = (int[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.ReadInt();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadLongArray(BinaryReader @in)
		{
			long[] v = (long[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.ReadLong();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadFloatArray(BinaryReader @in)
		{
			float[] v = (float[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.ReadFloat();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadDoubleArray(BinaryReader @in)
		{
			double[] v = (double[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.ReadDouble();
			}
		}
	}
}
