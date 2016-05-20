using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A polymorphic Writable that writes an instance with it's class name.</summary>
	/// <remarks>
	/// A polymorphic Writable that writes an instance with it's class name.
	/// Handles arrays, strings and primitive types without a Writable wrapper.
	/// </remarks>
	public class ObjectWritable : org.apache.hadoop.io.Writable, org.apache.hadoop.conf.Configurable
	{
		private java.lang.Class declaredClass;

		private object instance;

		private org.apache.hadoop.conf.Configuration conf;

		public ObjectWritable()
		{
		}

		public ObjectWritable(object instance)
		{
			set(instance);
		}

		public ObjectWritable(java.lang.Class declaredClass, object instance)
		{
			this.declaredClass = declaredClass;
			this.instance = instance;
		}

		/// <summary>Return the instance, or null if none.</summary>
		public virtual object get()
		{
			return instance;
		}

		/// <summary>Return the class this is meant to be.</summary>
		public virtual java.lang.Class getDeclaredClass()
		{
			return declaredClass;
		}

		/// <summary>Reset the instance.</summary>
		public virtual void set(object instance)
		{
			this.declaredClass = Sharpen.Runtime.getClassForObject(instance);
			this.instance = instance;
		}

		public override string ToString()
		{
			return "OW[class=" + declaredClass + ",value=" + instance + "]";
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			readObject(@in, this, this.conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			writeObject(@out, instance, declaredClass, conf);
		}

		private static readonly System.Collections.Generic.IDictionary<string, java.lang.Class
			> PRIMITIVE_NAMES = new System.Collections.Generic.Dictionary<string, java.lang.Class
			>();

		static ObjectWritable()
		{
			PRIMITIVE_NAMES["boolean"] = Sharpen.Runtime.getClassForType(typeof(bool));
			PRIMITIVE_NAMES["byte"] = Sharpen.Runtime.getClassForType(typeof(byte));
			PRIMITIVE_NAMES["char"] = Sharpen.Runtime.getClassForType(typeof(char));
			PRIMITIVE_NAMES["short"] = Sharpen.Runtime.getClassForType(typeof(short));
			PRIMITIVE_NAMES["int"] = Sharpen.Runtime.getClassForType(typeof(int));
			PRIMITIVE_NAMES["long"] = Sharpen.Runtime.getClassForType(typeof(long));
			PRIMITIVE_NAMES["float"] = Sharpen.Runtime.getClassForType(typeof(float));
			PRIMITIVE_NAMES["double"] = Sharpen.Runtime.getClassForType(typeof(double));
			PRIMITIVE_NAMES["void"] = Sharpen.Runtime.getClassForType(typeof(void));
		}

		private class NullInstance : org.apache.hadoop.conf.Configured, org.apache.hadoop.io.Writable
		{
			private java.lang.Class declaredClass;

			public NullInstance()
				: base(null)
			{
			}

			public NullInstance(java.lang.Class declaredClass, org.apache.hadoop.conf.Configuration
				 conf)
				: base(conf)
			{
				this.declaredClass = declaredClass;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
				string className = org.apache.hadoop.io.UTF8.readString(@in);
				declaredClass = PRIMITIVE_NAMES[className];
				if (declaredClass == null)
				{
					try
					{
						declaredClass = getConf().getClassByName(className);
					}
					catch (java.lang.ClassNotFoundException e)
					{
						throw new System.Exception(e.ToString());
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				org.apache.hadoop.io.UTF8.writeString(@out, declaredClass.getName());
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
		public static void writeObject(java.io.DataOutput @out, object instance, java.lang.Class
			 declaredClass, org.apache.hadoop.conf.Configuration conf)
		{
			writeObject(@out, instance, declaredClass, conf, false);
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
		public static void writeObject(java.io.DataOutput @out, object instance, java.lang.Class
			 declaredClass, org.apache.hadoop.conf.Configuration conf, bool allowCompactArrays
			)
		{
			if (instance == null)
			{
				// null
				instance = new org.apache.hadoop.io.ObjectWritable.NullInstance(declaredClass, conf
					);
				declaredClass = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Writable
					));
			}
			// Special case: must come before writing out the declaredClass.
			// If this is an eligible array of primitives,
			// wrap it in an ArrayPrimitiveWritable$Internal wrapper class.
			if (allowCompactArrays && declaredClass.isArray() && Sharpen.Runtime.getClassForObject
				(instance).getName().Equals(declaredClass.getName()) && Sharpen.Runtime.getClassForObject
				(instance).getComponentType().isPrimitive())
			{
				instance = new org.apache.hadoop.io.ArrayPrimitiveWritable.Internal(instance);
				declaredClass = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ArrayPrimitiveWritable.Internal
					));
			}
			org.apache.hadoop.io.UTF8.writeString(@out, declaredClass.getName());
			// always write declared
			if (declaredClass.isArray())
			{
				// non-primitive or non-compact array
				int length = java.lang.reflect.Array.getLength(instance);
				@out.writeInt(length);
				for (int i = 0; i < length; i++)
				{
					writeObject(@out, java.lang.reflect.Array.get(instance, i), declaredClass.getComponentType
						(), conf, allowCompactArrays);
				}
			}
			else
			{
				if (declaredClass == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ArrayPrimitiveWritable.Internal
					)))
				{
					((org.apache.hadoop.io.ArrayPrimitiveWritable.Internal)instance).write(@out);
				}
				else
				{
					if (declaredClass == Sharpen.Runtime.getClassForType(typeof(string)))
					{
						// String
						org.apache.hadoop.io.UTF8.writeString(@out, (string)instance);
					}
					else
					{
						if (declaredClass.isPrimitive())
						{
							// primitive type
							if (declaredClass == Sharpen.Runtime.getClassForType(typeof(bool)))
							{
								// boolean
								@out.writeBoolean(((bool)instance));
							}
							else
							{
								if (declaredClass == Sharpen.Runtime.getClassForType(typeof(char)))
								{
									// char
									@out.writeChar(((char)instance));
								}
								else
								{
									if (declaredClass == Sharpen.Runtime.getClassForType(typeof(byte)))
									{
										// byte
										@out.writeByte(((byte)instance));
									}
									else
									{
										if (declaredClass == Sharpen.Runtime.getClassForType(typeof(short)))
										{
											// short
											@out.writeShort(((short)instance));
										}
										else
										{
											if (declaredClass == Sharpen.Runtime.getClassForType(typeof(int)))
											{
												// int
												@out.writeInt(((int)instance));
											}
											else
											{
												if (declaredClass == Sharpen.Runtime.getClassForType(typeof(long)))
												{
													// long
													@out.writeLong(((long)instance));
												}
												else
												{
													if (declaredClass == Sharpen.Runtime.getClassForType(typeof(float)))
													{
														// float
														@out.writeFloat(((float)instance));
													}
													else
													{
														if (declaredClass == Sharpen.Runtime.getClassForType(typeof(double)))
														{
															// double
															@out.writeDouble(((double)instance));
														}
														else
														{
															if (declaredClass == Sharpen.Runtime.getClassForType(typeof(void)))
															{
															}
															else
															{
																// void
																throw new System.ArgumentException("Not a primitive: " + declaredClass);
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
							if (declaredClass.isEnum())
							{
								// enum
								org.apache.hadoop.io.UTF8.writeString(@out, ((java.lang.Enum)instance).name());
							}
							else
							{
								if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Writable)).isAssignableFrom
									(declaredClass))
								{
									// Writable
									org.apache.hadoop.io.UTF8.writeString(@out, Sharpen.Runtime.getClassForObject(instance
										).getName());
									((org.apache.hadoop.io.Writable)instance).write(@out);
								}
								else
								{
									if (Sharpen.Runtime.getClassForType(typeof(com.google.protobuf.Message)).isAssignableFrom
										(declaredClass))
									{
										((com.google.protobuf.Message)instance).writeDelimitedTo(org.apache.hadoop.io.DataOutputOutputStream
											.constructOutputStream(@out));
									}
									else
									{
										throw new System.IO.IOException("Can't write: " + instance + " as " + declaredClass
											);
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
		public static object readObject(java.io.DataInput @in, org.apache.hadoop.conf.Configuration
			 conf)
		{
			return readObject(@in, null, conf);
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
		public static object readObject(java.io.DataInput @in, org.apache.hadoop.io.ObjectWritable
			 objectWritable, org.apache.hadoop.conf.Configuration conf)
		{
			string className = org.apache.hadoop.io.UTF8.readString(@in);
			java.lang.Class declaredClass = PRIMITIVE_NAMES[className];
			if (declaredClass == null)
			{
				declaredClass = loadClass(conf, className);
			}
			object instance;
			if (declaredClass.isPrimitive())
			{
				// primitive types
				if (declaredClass == Sharpen.Runtime.getClassForType(typeof(bool)))
				{
					// boolean
					instance = bool.valueOf(@in.readBoolean());
				}
				else
				{
					if (declaredClass == Sharpen.Runtime.getClassForType(typeof(char)))
					{
						// char
						instance = char.valueOf(@in.readChar());
					}
					else
					{
						if (declaredClass == Sharpen.Runtime.getClassForType(typeof(byte)))
						{
							// byte
							instance = byte.valueOf(@in.readByte());
						}
						else
						{
							if (declaredClass == Sharpen.Runtime.getClassForType(typeof(short)))
							{
								// short
								instance = short.valueOf(@in.readShort());
							}
							else
							{
								if (declaredClass == Sharpen.Runtime.getClassForType(typeof(int)))
								{
									// int
									instance = int.Parse(@in.readInt());
								}
								else
								{
									if (declaredClass == Sharpen.Runtime.getClassForType(typeof(long)))
									{
										// long
										instance = long.valueOf(@in.readLong());
									}
									else
									{
										if (declaredClass == Sharpen.Runtime.getClassForType(typeof(float)))
										{
											// float
											instance = float.valueOf(@in.readFloat());
										}
										else
										{
											if (declaredClass == Sharpen.Runtime.getClassForType(typeof(double)))
											{
												// double
												instance = double.valueOf(@in.readDouble());
											}
											else
											{
												if (declaredClass == Sharpen.Runtime.getClassForType(typeof(void)))
												{
													// void
													instance = null;
												}
												else
												{
													throw new System.ArgumentException("Not a primitive: " + declaredClass);
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
				if (declaredClass.isArray())
				{
					// array
					int length = @in.readInt();
					instance = java.lang.reflect.Array.newInstance(declaredClass.getComponentType(), 
						length);
					for (int i = 0; i < length; i++)
					{
						java.lang.reflect.Array.set(instance, i, readObject(@in, conf));
					}
				}
				else
				{
					if (declaredClass == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ArrayPrimitiveWritable.Internal
						)))
					{
						// Read and unwrap ArrayPrimitiveWritable$Internal array.
						// Always allow the read, even if write is disabled by allowCompactArrays.
						org.apache.hadoop.io.ArrayPrimitiveWritable.Internal temp = new org.apache.hadoop.io.ArrayPrimitiveWritable.Internal
							();
						temp.readFields(@in);
						instance = temp.get();
						declaredClass = Sharpen.Runtime.getClassForObject(instance);
					}
					else
					{
						if (declaredClass == Sharpen.Runtime.getClassForType(typeof(string)))
						{
							// String
							instance = org.apache.hadoop.io.UTF8.readString(@in);
						}
						else
						{
							if (declaredClass.isEnum())
							{
								// enum
								instance = java.lang.Enum.valueOf((java.lang.Class)declaredClass, org.apache.hadoop.io.UTF8
									.readString(@in));
							}
							else
							{
								if (Sharpen.Runtime.getClassForType(typeof(com.google.protobuf.Message)).isAssignableFrom
									(declaredClass))
								{
									instance = tryInstantiateProtobuf(declaredClass, @in);
								}
								else
								{
									// Writable
									java.lang.Class instanceClass = null;
									string str = org.apache.hadoop.io.UTF8.readString(@in);
									instanceClass = loadClass(conf, str);
									org.apache.hadoop.io.Writable writable = org.apache.hadoop.io.WritableFactories.newInstance
										(instanceClass, conf);
									writable.readFields(@in);
									instance = writable;
									if (instanceClass == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ObjectWritable.NullInstance
										)))
									{
										// null
										declaredClass = ((org.apache.hadoop.io.ObjectWritable.NullInstance)instance).declaredClass;
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
		private static com.google.protobuf.Message tryInstantiateProtobuf(java.lang.Class
			 protoClass, java.io.DataInput dataIn)
		{
			try
			{
				if (dataIn is java.io.InputStream)
				{
					// We can use the built-in parseDelimitedFrom and not have to re-copy
					// the data
					java.lang.reflect.Method parseMethod = getStaticProtobufMethod(protoClass, "parseDelimitedFrom"
						, Sharpen.Runtime.getClassForType(typeof(java.io.InputStream)));
					return (com.google.protobuf.Message)parseMethod.invoke(null, (java.io.InputStream
						)dataIn);
				}
				else
				{
					// Have to read it into a buffer first, since protobuf doesn't deal
					// with the DataInput interface directly.
					// Read the size delimiter that writeDelimitedTo writes
					int size = org.apache.hadoop.util.ProtoUtil.readRawVarint32(dataIn);
					if (size < 0)
					{
						throw new System.IO.IOException("Invalid size: " + size);
					}
					byte[] data = new byte[size];
					dataIn.readFully(data);
					java.lang.reflect.Method parseMethod = getStaticProtobufMethod(protoClass, "parseFrom"
						, Sharpen.Runtime.getClassForType(typeof(byte[])));
					return (com.google.protobuf.Message)parseMethod.invoke(null, data);
				}
			}
			catch (java.lang.reflect.InvocationTargetException e)
			{
				if (e.InnerException is System.IO.IOException)
				{
					throw (System.IO.IOException)e.InnerException;
				}
				else
				{
					throw new System.IO.IOException(e.InnerException);
				}
			}
			catch (java.lang.IllegalAccessException)
			{
				throw new java.lang.AssertionError("Could not access parse method in " + protoClass
					);
			}
		}

		internal static java.lang.reflect.Method getStaticProtobufMethod(java.lang.Class 
			declaredClass, string method, params java.lang.Class[] args)
		{
			try
			{
				return declaredClass.getMethod(method, args);
			}
			catch (System.Exception)
			{
				// This is a bug in Hadoop - protobufs should all have this static method
				throw new java.lang.AssertionError("Protocol buffer class " + declaredClass + " does not have an accessible parseFrom(InputStream) method!"
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
		public static java.lang.Class loadClass(org.apache.hadoop.conf.Configuration conf
			, string className)
		{
			java.lang.Class declaredClass = null;
			try
			{
				if (conf != null)
				{
					declaredClass = conf.getClassByName(className);
				}
				else
				{
					declaredClass = java.lang.Class.forName(className);
				}
			}
			catch (java.lang.ClassNotFoundException e)
			{
				throw new System.Exception("readObject can't find class " + className, e);
			}
			return declaredClass;
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return this.conf;
		}
	}
}
