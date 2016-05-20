using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>This is a wrapper class.</summary>
	/// <remarks>
	/// This is a wrapper class.  It wraps a Writable implementation around
	/// an array of primitives (e.g., int[], long[], etc.), with optimized
	/// wire format, and without creating new objects per element.
	/// This is a wrapper class only; it does not make a copy of the
	/// underlying array.
	/// </remarks>
	public class ArrayPrimitiveWritable : org.apache.hadoop.io.Writable
	{
		private java.lang.Class componentType = null;

		private java.lang.Class declaredComponentType = null;

		private int length;

		private object value;

		private static readonly System.Collections.Generic.IDictionary<string, java.lang.Class
			> PRIMITIVE_NAMES = new System.Collections.Generic.Dictionary<string, java.lang.Class
			>(16);

		static ArrayPrimitiveWritable()
		{
			//componentType is determined from the component type of the value array 
			//during a "set" operation.  It must be primitive.
			//declaredComponentType need not be declared, but if you do (by using the
			//ArrayPrimitiveWritable(Class<?>) constructor), it will provide typechecking
			//for all "set" operations.
			//must be an array of <componentType>[length]
			PRIMITIVE_NAMES[Sharpen.Runtime.getClassForType(typeof(bool)).getName()] = Sharpen.Runtime.getClassForType
				(typeof(bool));
			PRIMITIVE_NAMES[Sharpen.Runtime.getClassForType(typeof(byte)).getName()] = Sharpen.Runtime.getClassForType
				(typeof(byte));
			PRIMITIVE_NAMES[Sharpen.Runtime.getClassForType(typeof(char)).getName()] = Sharpen.Runtime.getClassForType
				(typeof(char));
			PRIMITIVE_NAMES[Sharpen.Runtime.getClassForType(typeof(short)).getName()] = Sharpen.Runtime.getClassForType
				(typeof(short));
			PRIMITIVE_NAMES[Sharpen.Runtime.getClassForType(typeof(int)).getName()] = Sharpen.Runtime.getClassForType
				(typeof(int));
			PRIMITIVE_NAMES[Sharpen.Runtime.getClassForType(typeof(long)).getName()] = Sharpen.Runtime.getClassForType
				(typeof(long));
			PRIMITIVE_NAMES[Sharpen.Runtime.getClassForType(typeof(float)).getName()] = Sharpen.Runtime.getClassForType
				(typeof(float));
			PRIMITIVE_NAMES[Sharpen.Runtime.getClassForType(typeof(double)).getName()] = Sharpen.Runtime.getClassForType
				(typeof(double));
		}

		private static java.lang.Class getPrimitiveClass(string className)
		{
			return PRIMITIVE_NAMES[className];
		}

		private static void checkPrimitive(java.lang.Class componentType)
		{
			if (componentType == null)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("null component type not allowed"
					);
			}
			if (!PRIMITIVE_NAMES.Contains(componentType.getName()))
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("input array component type "
					 + componentType.getName() + " is not a candidate primitive type");
			}
		}

		private void checkDeclaredComponentType(java.lang.Class componentType)
		{
			if ((declaredComponentType != null) && (componentType != declaredComponentType))
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("input array component type "
					 + componentType.getName() + " does not match declared type " + declaredComponentType
					.getName());
			}
		}

		private static void checkArray(object value)
		{
			if (value == null)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("null value not allowed"
					);
			}
			if (!Sharpen.Runtime.getClassForObject(value).isArray())
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("non-array value of class "
					 + Sharpen.Runtime.getClassForObject(value) + " not allowed");
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
		public ArrayPrimitiveWritable(java.lang.Class componentType)
		{
			//empty constructor
			checkPrimitive(componentType);
			this.declaredComponentType = componentType;
		}

		/// <summary>Wrap an existing array of primitives</summary>
		/// <param name="value">- array of primitives</param>
		public ArrayPrimitiveWritable(object value)
		{
			set(value);
		}

		/// <summary>Get the original array.</summary>
		/// <remarks>
		/// Get the original array.
		/// Client must cast it back to type componentType[]
		/// (or may use type-specific wrapper classes).
		/// </remarks>
		/// <returns>- original array as Object</returns>
		public virtual object get()
		{
			return value;
		}

		public virtual java.lang.Class getComponentType()
		{
			return componentType;
		}

		public virtual java.lang.Class getDeclaredComponentType()
		{
			return declaredComponentType;
		}

		public virtual bool isDeclaredComponentType(java.lang.Class componentType)
		{
			return componentType == declaredComponentType;
		}

		public virtual void set(object value)
		{
			checkArray(value);
			java.lang.Class componentType = Sharpen.Runtime.getClassForObject(value).getComponentType
				();
			checkPrimitive(componentType);
			checkDeclaredComponentType(componentType);
			this.componentType = componentType;
			this.value = value;
			this.length = java.lang.reflect.Array.getLength(value);
		}

		/// <summary>Do not use this class.</summary>
		/// <remarks>
		/// Do not use this class.
		/// This is an internal class, purely for ObjectWritable to use as
		/// a label class for transparent conversions of arrays of primitives
		/// during wire protocol reads and writes.
		/// </remarks>
		internal class Internal : org.apache.hadoop.io.ArrayPrimitiveWritable
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
		public virtual void write(java.io.DataOutput @out)
		{
			// write componentType 
			org.apache.hadoop.io.UTF8.writeString(@out, componentType.getName());
			// write length
			@out.writeInt(length);
			// do the inner loop.  Walk the decision tree only once.
			if (componentType == Sharpen.Runtime.getClassForType(typeof(bool)))
			{
				// boolean
				writeBooleanArray(@out);
			}
			else
			{
				if (componentType == Sharpen.Runtime.getClassForType(typeof(char)))
				{
					// char
					writeCharArray(@out);
				}
				else
				{
					if (componentType == Sharpen.Runtime.getClassForType(typeof(byte)))
					{
						// byte
						writeByteArray(@out);
					}
					else
					{
						if (componentType == Sharpen.Runtime.getClassForType(typeof(short)))
						{
							// short
							writeShortArray(@out);
						}
						else
						{
							if (componentType == Sharpen.Runtime.getClassForType(typeof(int)))
							{
								// int
								writeIntArray(@out);
							}
							else
							{
								if (componentType == Sharpen.Runtime.getClassForType(typeof(long)))
								{
									// long
									writeLongArray(@out);
								}
								else
								{
									if (componentType == Sharpen.Runtime.getClassForType(typeof(float)))
									{
										// float
										writeFloatArray(@out);
									}
									else
									{
										if (componentType == Sharpen.Runtime.getClassForType(typeof(double)))
										{
											// double
											writeDoubleArray(@out);
										}
										else
										{
											throw new System.IO.IOException("Component type " + componentType.ToString() + " is set as the output type, but no encoding is implemented for this type."
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
		* @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			// read and set the component type of the array
			string className = org.apache.hadoop.io.UTF8.readString(@in);
			java.lang.Class componentType = getPrimitiveClass(className);
			if (componentType == null)
			{
				throw new System.IO.IOException("encoded array component type " + className + " is not a candidate primitive type"
					);
			}
			checkDeclaredComponentType(componentType);
			this.componentType = componentType;
			// read and set the length of the array
			int length = @in.readInt();
			if (length < 0)
			{
				throw new System.IO.IOException("encoded array length is negative " + length);
			}
			this.length = length;
			// construct and read in the array
			value = java.lang.reflect.Array.newInstance(componentType, length);
			// do the inner loop.  Walk the decision tree only once.
			if (componentType == Sharpen.Runtime.getClassForType(typeof(bool)))
			{
				// boolean
				readBooleanArray(@in);
			}
			else
			{
				if (componentType == Sharpen.Runtime.getClassForType(typeof(char)))
				{
					// char
					readCharArray(@in);
				}
				else
				{
					if (componentType == Sharpen.Runtime.getClassForType(typeof(byte)))
					{
						// byte
						readByteArray(@in);
					}
					else
					{
						if (componentType == Sharpen.Runtime.getClassForType(typeof(short)))
						{
							// short
							readShortArray(@in);
						}
						else
						{
							if (componentType == Sharpen.Runtime.getClassForType(typeof(int)))
							{
								// int
								readIntArray(@in);
							}
							else
							{
								if (componentType == Sharpen.Runtime.getClassForType(typeof(long)))
								{
									// long
									readLongArray(@in);
								}
								else
								{
									if (componentType == Sharpen.Runtime.getClassForType(typeof(float)))
									{
										// float
										readFloatArray(@in);
									}
									else
									{
										if (componentType == Sharpen.Runtime.getClassForType(typeof(double)))
										{
											// double
											readDoubleArray(@in);
										}
										else
										{
											throw new System.IO.IOException("Encoded type " + className + " converted to valid component type "
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
		private void writeBooleanArray(java.io.DataOutput @out)
		{
			bool[] v = (bool[])value;
			for (int i = 0; i < length; i++)
			{
				@out.writeBoolean(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeCharArray(java.io.DataOutput @out)
		{
			char[] v = (char[])value;
			for (int i = 0; i < length; i++)
			{
				@out.writeChar(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeByteArray(java.io.DataOutput @out)
		{
			@out.write((byte[])value, 0, length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeShortArray(java.io.DataOutput @out)
		{
			short[] v = (short[])value;
			for (int i = 0; i < length; i++)
			{
				@out.writeShort(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeIntArray(java.io.DataOutput @out)
		{
			int[] v = (int[])value;
			for (int i = 0; i < length; i++)
			{
				@out.writeInt(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeLongArray(java.io.DataOutput @out)
		{
			long[] v = (long[])value;
			for (int i = 0; i < length; i++)
			{
				@out.writeLong(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeFloatArray(java.io.DataOutput @out)
		{
			float[] v = (float[])value;
			for (int i = 0; i < length; i++)
			{
				@out.writeFloat(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeDoubleArray(java.io.DataOutput @out)
		{
			double[] v = (double[])value;
			for (int i = 0; i < length; i++)
			{
				@out.writeDouble(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readBooleanArray(java.io.DataInput @in)
		{
			bool[] v = (bool[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.readBoolean();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readCharArray(java.io.DataInput @in)
		{
			char[] v = (char[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.readChar();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readByteArray(java.io.DataInput @in)
		{
			@in.readFully((byte[])value, 0, length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void readShortArray(java.io.DataInput @in)
		{
			short[] v = (short[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.readShort();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readIntArray(java.io.DataInput @in)
		{
			int[] v = (int[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.readInt();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readLongArray(java.io.DataInput @in)
		{
			long[] v = (long[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.readLong();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readFloatArray(java.io.DataInput @in)
		{
			float[] v = (float[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.readFloat();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readDoubleArray(java.io.DataInput @in)
		{
			double[] v = (double[])value;
			for (int i = 0; i < length; i++)
			{
				v[i] = @in.readDouble();
			}
		}
	}
}
