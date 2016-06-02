using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Com.Google.Protobuf;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A polymorphic Writable that writes an instance with it's class name.</summary>
	/// <remarks>
	/// A polymorphic Writable that writes an instance with it's class name.
	/// Handles arrays, strings and primitive types without a Writable wrapper.
	/// </remarks>
	public class ObjectWritable : Writable, Configurable
	{
		private Type declaredClass;

		private object instance;

		private Configuration conf;

		public ObjectWritable()
		{
		}

		public ObjectWritable(object instance)
		{
			Set(instance);
		}

		public ObjectWritable(Type declaredClass, object instance)
		{
			this.declaredClass = declaredClass;
			this.instance = instance;
		}

		/// <summary>Return the instance, or null if none.</summary>
		public virtual object Get()
		{
			return instance;
		}

		/// <summary>Return the class this is meant to be.</summary>
		public virtual Type GetDeclaredClass()
		{
			return declaredClass;
		}

		/// <summary>Reset the instance.</summary>
		public virtual void Set(object instance)
		{
			this.declaredClass = instance.GetType();
			this.instance = instance;
		}

		public override string ToString()
		{
			return "OW[class=" + declaredClass + ",value=" + instance + "]";
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			ReadObject(@in, this, this.conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			WriteObject(@out, instance, declaredClass, conf);
		}

		private static readonly IDictionary<string, Type> PrimitiveNames = new Dictionary
			<string, Type>();

		static ObjectWritable()
		{
			PrimitiveNames["boolean"] = typeof(bool);
			PrimitiveNames["byte"] = typeof(byte);
			PrimitiveNames["char"] = typeof(char);
			PrimitiveNames["short"] = typeof(short);
			PrimitiveNames["int"] = typeof(int);
			PrimitiveNames["long"] = typeof(long);
			PrimitiveNames["float"] = typeof(float);
			PrimitiveNames["double"] = typeof(double);
			PrimitiveNames["void"] = typeof(void);
		}

		private class NullInstance : Configured, Writable
		{
			private Type declaredClass;

			public NullInstance()
				: base(null)
			{
			}

			public NullInstance(Type declaredClass, Configuration conf)
				: base(conf)
			{
				this.declaredClass = declaredClass;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				string className = UTF8.ReadString(@in);
				declaredClass = PrimitiveNames[className];
				if (declaredClass == null)
				{
					try
					{
						declaredClass = GetConf().GetClassByName(className);
					}
					catch (TypeLoadException e)
					{
						throw new RuntimeException(e.ToString());
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				UTF8.WriteString(@out, declaredClass.FullName);
			}
		}

		/// <summary>
		/// Write a
		/// <see cref="Writable"/>
		/// ,
		/// <see cref="string"/>
		/// , primitive type, or an array of
		/// the preceding.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteObject(DataOutput @out, object instance, Type declaredClass
			, Configuration conf)
		{
			WriteObject(@out, instance, declaredClass, conf, false);
		}

		/// <summary>
		/// Write a
		/// <see cref="Writable"/>
		/// ,
		/// <see cref="string"/>
		/// , primitive type, or an array of
		/// the preceding.
		/// </summary>
		/// <param name="allowCompactArrays">
		/// - set true for RPC and internal or intra-cluster
		/// usages.  Set false for inter-cluster, File, and other persisted output
		/// usages, to preserve the ability to interchange files with other clusters
		/// that may not be running the same version of software.  Sometime in ~2013
		/// we can consider removing this parameter and always using the compact format.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteObject(DataOutput @out, object instance, Type declaredClass
			, Configuration conf, bool allowCompactArrays)
		{
			if (instance == null)
			{
				// null
				instance = new ObjectWritable.NullInstance(declaredClass, conf);
				declaredClass = typeof(Writable);
			}
			// Special case: must come before writing out the declaredClass.
			// If this is an eligible array of primitives,
			// wrap it in an ArrayPrimitiveWritable$Internal wrapper class.
			if (allowCompactArrays && declaredClass.IsArray && instance.GetType().FullName.Equals
				(declaredClass.FullName) && instance.GetType().GetElementType().IsPrimitive)
			{
				instance = new ArrayPrimitiveWritable.Internal(instance);
				declaredClass = typeof(ArrayPrimitiveWritable.Internal);
			}
			UTF8.WriteString(@out, declaredClass.FullName);
			// always write declared
			if (declaredClass.IsArray)
			{
				// non-primitive or non-compact array
				int length = Sharpen.Runtime.GetArrayLength(instance);
				@out.WriteInt(length);
				for (int i = 0; i < length; i++)
				{
					WriteObject(@out, Sharpen.Runtime.GetArrayValue(instance, i), declaredClass.GetElementType
						(), conf, allowCompactArrays);
				}
			}
			else
			{
				if (declaredClass == typeof(ArrayPrimitiveWritable.Internal))
				{
					((ArrayPrimitiveWritable.Internal)instance).Write(@out);
				}
				else
				{
					if (declaredClass == typeof(string))
					{
						// String
						UTF8.WriteString(@out, (string)instance);
					}
					else
					{
						if (declaredClass.IsPrimitive)
						{
							// primitive type
							if (declaredClass == typeof(bool))
							{
								// boolean
								@out.WriteBoolean(((bool)instance));
							}
							else
							{
								if (declaredClass == typeof(char))
								{
									// char
									@out.WriteChar(((char)instance));
								}
								else
								{
									if (declaredClass == typeof(byte))
									{
										// byte
										@out.WriteByte(((byte)instance));
									}
									else
									{
										if (declaredClass == typeof(short))
										{
											// short
											@out.WriteShort(((short)instance));
										}
										else
										{
											if (declaredClass == typeof(int))
											{
												// int
												@out.WriteInt(((int)instance));
											}
											else
											{
												if (declaredClass == typeof(long))
												{
													// long
													@out.WriteLong(((long)instance));
												}
												else
												{
													if (declaredClass == typeof(float))
													{
														// float
														@out.WriteFloat(((float)instance));
													}
													else
													{
														if (declaredClass == typeof(double))
														{
															// double
															@out.WriteDouble(((double)instance));
														}
														else
														{
															if (declaredClass == typeof(void))
															{
															}
															else
															{
																// void
																throw new ArgumentException("Not a primitive: " + declaredClass);
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
						else
						{
							if (declaredClass.IsEnum())
							{
								// enum
								UTF8.WriteString(@out, ((Enum)instance).Name());
							}
							else
							{
								if (typeof(Writable).IsAssignableFrom(declaredClass))
								{
									// Writable
									UTF8.WriteString(@out, instance.GetType().FullName);
									((Writable)instance).Write(@out);
								}
								else
								{
									if (typeof(Message).IsAssignableFrom(declaredClass))
									{
										((Message)instance).WriteDelimitedTo(DataOutputOutputStream.ConstructOutputStream
											(@out));
									}
									else
									{
										throw new IOException("Can't write: " + instance + " as " + declaredClass);
									}
								}
							}
						}
					}
				}
			}
		}

		/// <summary>
		/// Read a
		/// <see cref="Writable"/>
		/// ,
		/// <see cref="string"/>
		/// , primitive type, or an array of
		/// the preceding.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static object ReadObject(DataInput @in, Configuration conf)
		{
			return ReadObject(@in, null, conf);
		}

		/// <summary>
		/// Read a
		/// <see cref="Writable"/>
		/// ,
		/// <see cref="string"/>
		/// , primitive type, or an array of
		/// the preceding.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static object ReadObject(DataInput @in, ObjectWritable objectWritable, Configuration
			 conf)
		{
			string className = UTF8.ReadString(@in);
			Type declaredClass = PrimitiveNames[className];
			if (declaredClass == null)
			{
				declaredClass = LoadClass(conf, className);
			}
			object instance;
			if (declaredClass.IsPrimitive)
			{
				// primitive types
				if (declaredClass == typeof(bool))
				{
					// boolean
					instance = Sharpen.Extensions.ValueOf(@in.ReadBoolean());
				}
				else
				{
					if (declaredClass == typeof(char))
					{
						// char
						instance = char.ValueOf(@in.ReadChar());
					}
					else
					{
						if (declaredClass == typeof(byte))
						{
							// byte
							instance = byte.ValueOf(@in.ReadByte());
						}
						else
						{
							if (declaredClass == typeof(short))
							{
								// short
								instance = short.ValueOf(@in.ReadShort());
							}
							else
							{
								if (declaredClass == typeof(int))
								{
									// int
									instance = Sharpen.Extensions.ValueOf(@in.ReadInt());
								}
								else
								{
									if (declaredClass == typeof(long))
									{
										// long
										instance = Sharpen.Extensions.ValueOf(@in.ReadLong());
									}
									else
									{
										if (declaredClass == typeof(float))
										{
											// float
											instance = float.ValueOf(@in.ReadFloat());
										}
										else
										{
											if (declaredClass == typeof(double))
											{
												// double
												instance = double.ValueOf(@in.ReadDouble());
											}
											else
											{
												if (declaredClass == typeof(void))
												{
													// void
													instance = null;
												}
												else
												{
													throw new ArgumentException("Not a primitive: " + declaredClass);
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			else
			{
				if (declaredClass.IsArray)
				{
					// array
					int length = @in.ReadInt();
					instance = System.Array.CreateInstance(declaredClass.GetElementType(), length);
					for (int i = 0; i < length; i++)
					{
						Sharpen.Runtime.SetArrayValue(instance, i, ReadObject(@in, conf));
					}
				}
				else
				{
					if (declaredClass == typeof(ArrayPrimitiveWritable.Internal))
					{
						// Read and unwrap ArrayPrimitiveWritable$Internal array.
						// Always allow the read, even if write is disabled by allowCompactArrays.
						ArrayPrimitiveWritable.Internal temp = new ArrayPrimitiveWritable.Internal();
						temp.ReadFields(@in);
						instance = temp.Get();
						declaredClass = instance.GetType();
					}
					else
					{
						if (declaredClass == typeof(string))
						{
							// String
							instance = UTF8.ReadString(@in);
						}
						else
						{
							if (declaredClass.IsEnum())
							{
								// enum
								instance = Enum.ValueOf((Type)declaredClass, UTF8.ReadString(@in));
							}
							else
							{
								if (typeof(Message).IsAssignableFrom(declaredClass))
								{
									instance = TryInstantiateProtobuf(declaredClass, @in);
								}
								else
								{
									// Writable
									Type instanceClass = null;
									string str = UTF8.ReadString(@in);
									instanceClass = LoadClass(conf, str);
									Writable writable = WritableFactories.NewInstance(instanceClass, conf);
									writable.ReadFields(@in);
									instance = writable;
									if (instanceClass == typeof(ObjectWritable.NullInstance))
									{
										// null
										declaredClass = ((ObjectWritable.NullInstance)instance).declaredClass;
										instance = null;
									}
								}
							}
						}
					}
				}
			}
			if (objectWritable != null)
			{
				// store values
				objectWritable.declaredClass = declaredClass;
				objectWritable.instance = instance;
			}
			return instance;
		}

		/// <summary>
		/// Try to instantiate a protocol buffer of the given message class
		/// from the given input stream.
		/// </summary>
		/// <param name="protoClass">the class of the generated protocol buffer</param>
		/// <param name="dataIn">the input stream to read from</param>
		/// <returns>the instantiated Message instance</returns>
		/// <exception cref="System.IO.IOException">if an IO problem occurs</exception>
		private static Message TryInstantiateProtobuf(Type protoClass, DataInput dataIn)
		{
			try
			{
				if (dataIn is InputStream)
				{
					// We can use the built-in parseDelimitedFrom and not have to re-copy
					// the data
					MethodInfo parseMethod = GetStaticProtobufMethod(protoClass, "parseDelimitedFrom"
						, typeof(InputStream));
					return (Message)parseMethod.Invoke(null, (InputStream)dataIn);
				}
				else
				{
					// Have to read it into a buffer first, since protobuf doesn't deal
					// with the DataInput interface directly.
					// Read the size delimiter that writeDelimitedTo writes
					int size = ProtoUtil.ReadRawVarint32(dataIn);
					if (size < 0)
					{
						throw new IOException("Invalid size: " + size);
					}
					byte[] data = new byte[size];
					dataIn.ReadFully(data);
					MethodInfo parseMethod = GetStaticProtobufMethod(protoClass, "parseFrom", typeof(
						byte[]));
					return (Message)parseMethod.Invoke(null, data);
				}
			}
			catch (TargetInvocationException e)
			{
				if (e.InnerException is IOException)
				{
					throw (IOException)e.InnerException;
				}
				else
				{
					throw new IOException(e.InnerException);
				}
			}
			catch (MemberAccessException)
			{
				throw new Exception("Could not access parse method in " + protoClass);
			}
		}

		internal static MethodInfo GetStaticProtobufMethod(Type declaredClass, string method
			, params Type[] args)
		{
			try
			{
				return declaredClass.GetMethod(method, args);
			}
			catch (Exception)
			{
				// This is a bug in Hadoop - protobufs should all have this static method
				throw new Exception("Protocol buffer class " + declaredClass + " does not have an accessible parseFrom(InputStream) method!"
					);
			}
		}

		/// <summary>
		/// Find and load the class with given name <tt>className</tt> by first finding
		/// it in the specified <tt>conf</tt>.
		/// </summary>
		/// <remarks>
		/// Find and load the class with given name <tt>className</tt> by first finding
		/// it in the specified <tt>conf</tt>. If the specified <tt>conf</tt> is null,
		/// try load it directly.
		/// </remarks>
		public static Type LoadClass(Configuration conf, string className)
		{
			Type declaredClass = null;
			try
			{
				if (conf != null)
				{
					declaredClass = conf.GetClassByName(className);
				}
				else
				{
					declaredClass = Sharpen.Runtime.GetType(className);
				}
			}
			catch (TypeLoadException e)
			{
				throw new RuntimeException("readObject can't find class " + className, e);
			}
			return declaredClass;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return this.conf;
		}
	}
}
