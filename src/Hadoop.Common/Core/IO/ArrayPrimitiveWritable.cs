using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop;

namespace Hadoop.Common.Core.IO
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
		private Type _componentType;

		private readonly Type _declaredComponentType;

		private int _length;

		private object _value;

		private static readonly IDictionary<string, Type> PrimitiveNames = new Dictionary<string, Type>(16);

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
			if (!PrimitiveNames.ContainsKey(componentType.FullName))
			{
				throw new HadoopIllegalArgumentException("input array component type " + componentType.FullName + " is not a candidate primitive type");
			}
		}

		private void CheckDeclaredComponentType(Type componentType)
		{
			if ((_declaredComponentType != null) && (componentType != _declaredComponentType))
			{
				throw new HadoopIllegalArgumentException("input array component type " + componentType
					.FullName + " does not match declared type " + _declaredComponentType.FullName);
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
				throw new HadoopIllegalArgumentException("non-array value of class " + value.GetType() + " not allowed");
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
			this._declaredComponentType = componentType;
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
			return _value;
		}

		public virtual Type GetComponentType()
		{
			return _componentType;
		}

		public virtual Type GetDeclaredComponentType()
		{
			return _declaredComponentType;
		}

		public virtual bool IsDeclaredComponentType(Type componentType)
		{
			return componentType == _declaredComponentType;
		}

		public virtual void Set(object value)
		{
			CheckArray(value);
			Type componentType = value.GetType().GetElementType();
			CheckPrimitive(componentType);
			CheckDeclaredComponentType(componentType);
			_componentType = componentType;
			_value = value;
		    Array arr = value as Array;
		    _length = arr.Length;
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
		* @see org.apache.hadoop.io.Writable#write(java.io.BinaryWriter)
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			// write componentType 
			UTF8.WriteString(writer, _componentType.FullName);
			// write length
			writer.Write(_length);

            // do the inner loop.  Walk the decision tree only once.
		    switch (_componentType.Name)
		    {
                case "Boolean":
                    WriteBooleanArray(writer);
                    break;
                case "Char":
                    WriteCharArray(writer);
                    break;
                case "Byte":
                    WriteByteArray(writer);
                    break;
                case "Int16":
                    WriteShortArray(writer);
                    break;
                case "Int32":
                    WriteIntArray(writer);
                    break;
                case "Int64":
                    WriteLongArray(writer);
                    break;
                case "Single":
                    WriteFloatArray(writer);
                    break;
                case "Double":
                    WriteDoubleArray(writer);
                    break;
                default:
                    throw new IOException("Component type " + _componentType + " is set as the output type, but no encoding is implemented for this type.");
            }
            
		}
	   

	    /*
		* @see org.apache.hadoop.io.Writable#readFields(java.io.BinaryReader)
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			// read and set the component type of the array
			string className = UTF8.ReadString(reader);
			Type componentType = GetPrimitiveClass(className);
			if (componentType == null)
			{
				throw new IOException("encoded array component type " + className + " is not a candidate primitive type");
			}
			CheckDeclaredComponentType(componentType);
			this._componentType = componentType;
			// read and set the length of the array
			int length = reader.Read();
			if (length < 0)
			{
				throw new IOException("encoded array length is negative " + length);
			}
			this._length = length;
			// construct and read in the array
			_value = System.Array.CreateInstance(componentType, length);
            // do the inner loop.  Walk the decision tree only once.

            switch (componentType.Name)
            {
                case "Boolean":
                    ReadBooleanArray(reader);
                    break;
                case "Char":
                    ReadCharArray(reader);
                    break;
                case "Byte":
                    ReadByteArray(reader);
                    break;
                case "Int16":
                    ReadShortArray(reader);
                    break;
                case "Int32":
                    ReadIntArray(reader);
                    break;
                case "Int64":
                    ReadLongArray(reader);
                    break;
                case "Single":
                    ReadFloatArray(reader);
                    break;
                case "Double":
                    ReadDoubleArray(reader);
                    break;
                default:
                    throw new IOException("Encoded type " + className + " converted to valid component type "
                                                 + componentType + " but no encoding is implemented for this type.");
            }
            
		}

        //For efficient implementation, there's no way around
        //the following massive code duplication.
        /// <exception cref="System.IO.IOException"/>
        private void WriteBooleanArray(BinaryWriter writer)
		{
			bool[] v = (bool[])_value;
			for (int i = 0; i < _length; i++)
			{
				writer.Write(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteCharArray(BinaryWriter writer)
		{
			char[] v = (char[])_value;
			for (int i = 0; i < _length; i++)
			{
				writer.Write(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteByteArray(BinaryWriter writer)
		{
			writer.Write((byte[])_value, 0, _length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteShortArray(BinaryWriter writer)
		{
			short[] v = (short[])_value;
			for (int i = 0; i < _length; i++)
			{
				writer.Write(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteIntArray(BinaryWriter writer)
		{
			int[] v = (int[])_value;
			for (int i = 0; i < _length; i++)
			{
				writer.Write(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteLongArray(BinaryWriter writer)
		{
			long[] v = (long[])_value;
			for (int i = 0; i < _length; i++)
			{
				writer.Write(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFloatArray(BinaryWriter writer)
		{
			float[] v = (float[])_value;
			for (int i = 0; i < _length; i++)
			{
				writer.Write(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteDoubleArray(BinaryWriter writer)
		{
			double[] v = (double[])_value;
			for (int i = 0; i < _length; i++)
			{
				writer.Write(v[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadBooleanArray(BinaryReader reader)
		{
			bool[] v = (bool[])_value;
			for (int i = 0; i < _length; i++)
			{
				v[i] = reader.ReadBoolean();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadCharArray(BinaryReader reader)
		{
			char[] v = (char[])_value;
			for (int i = 0; i < _length; i++)
			{
				v[i] = reader.ReadChar();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadByteArray(BinaryReader reader)
		{
			_value = reader.ReadBytes(_length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadShortArray(BinaryReader reader)
		{
			short[] v = (short[])_value;
			for (int i = 0; i < _length; i++)
			{
				v[i] = (short) reader.Read();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadIntArray(BinaryReader reader)
		{
			int[] v = (int[])_value;
			for (int i = 0; i < _length; i++)
			{
				v[i] = reader.Read();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadLongArray(BinaryReader reader)
		{
			long[] v = (long[])_value;
			for (int i = 0; i < _length; i++)
			{
				v[i] = reader.Read();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadFloatArray(BinaryReader reader)
		{
			float[] v = (float[])_value;
			for (int i = 0; i < _length; i++)
			{
				v[i] = reader.Read();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadDoubleArray(BinaryReader reader)
		{
			double[] v = (double[])_value;
			for (int i = 0; i < _length; i++)
			{
				v[i] = reader.ReadDouble();
			}
		}
	}
}
