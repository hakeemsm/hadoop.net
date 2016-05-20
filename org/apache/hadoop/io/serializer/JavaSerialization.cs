using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	/// <summary>
	/// <p>
	/// An experimental
	/// <see cref="Serialization{T}"/>
	/// for Java
	/// <see cref="java.io.Serializable"/>
	/// classes.
	/// </p>
	/// </summary>
	/// <seealso cref="JavaSerializationComparator{T}"/>
	public class JavaSerialization : org.apache.hadoop.io.serializer.Serialization<java.io.Serializable
		>
	{
		internal class JavaSerializationDeserializer<T> : org.apache.hadoop.io.serializer.Deserializer
			<T>
			where T : java.io.Serializable
		{
			private java.io.ObjectInputStream ois;

			/// <exception cref="System.IO.IOException"/>
			public virtual void open(java.io.InputStream @in)
			{
				ois = new _ObjectInputStream_47(@in);
			}

			private sealed class _ObjectInputStream_47 : java.io.ObjectInputStream
			{
				public _ObjectInputStream_47(java.io.InputStream baseArg1)
					: base(baseArg1)
				{
				}

				protected override void readStreamHeader()
				{
				}
			}

			// no header
			/// <exception cref="System.IO.IOException"/>
			public virtual T deserialize(T @object)
			{
				try
				{
					// ignore passed-in object
					return (T)ois.readObject();
				}
				catch (java.lang.ClassNotFoundException e)
				{
					throw new System.IO.IOException(e.ToString());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				ois.close();
			}
		}

		internal class JavaSerializationSerializer : org.apache.hadoop.io.serializer.Serializer
			<java.io.Serializable>
		{
			private java.io.ObjectOutputStream oos;

			/// <exception cref="System.IO.IOException"/>
			public virtual void open(java.io.OutputStream @out)
			{
				oos = new _ObjectOutputStream_79(@out);
			}

			private sealed class _ObjectOutputStream_79 : java.io.ObjectOutputStream
			{
				public _ObjectOutputStream_79(java.io.OutputStream baseArg1)
					: base(baseArg1)
				{
				}

				protected override void writeStreamHeader()
				{
				}
			}

			// no header
			/// <exception cref="System.IO.IOException"/>
			public virtual void serialize(java.io.Serializable @object)
			{
				oos.reset();
				// clear (class) back-references
				oos.writeObject(@object);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				oos.close();
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual bool accept(java.lang.Class c)
		{
			return Sharpen.Runtime.getClassForType(typeof(java.io.Serializable)).isAssignableFrom
				(c);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual org.apache.hadoop.io.serializer.Deserializer<java.io.Serializable>
			 getDeserializer(java.lang.Class c)
		{
			return new org.apache.hadoop.io.serializer.JavaSerialization.JavaSerializationDeserializer
				<java.io.Serializable>();
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual org.apache.hadoop.io.serializer.Serializer<java.io.Serializable> getSerializer
			(java.lang.Class c)
		{
			return new org.apache.hadoop.io.serializer.JavaSerialization.JavaSerializationSerializer
				();
		}
	}
}
