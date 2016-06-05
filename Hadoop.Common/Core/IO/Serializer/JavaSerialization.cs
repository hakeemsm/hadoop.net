using System;
using System.IO;
using Org.Apache.Hadoop.Classification;


namespace Org.Apache.Hadoop.IO.Serializer
{
	/// <summary>
	/// <p>
	/// An experimental
	/// <see cref="Serialization{T}"/>
	/// for Java
	/// <see cref="System.IO.Serializable"/>
	/// classes.
	/// </p>
	/// </summary>
	/// <seealso cref="JavaSerializationComparator{T}"/>
	public class JavaSerialization : Serialization<Serializable>
	{
		internal class JavaSerializationDeserializer<T> : Deserializer<T>
			where T : Serializable
		{
			private ObjectInputStream ois;

			/// <exception cref="System.IO.IOException"/>
			public virtual void Open(InputStream @in)
			{
				ois = new _ObjectInputStream_47(@in);
			}

			private sealed class _ObjectInputStream_47 : ObjectInputStream
			{
				public _ObjectInputStream_47(InputStream baseArg1)
					: base(baseArg1)
				{
				}

				protected override void ReadStreamHeader()
				{
				}
			}

			// no header
			/// <exception cref="System.IO.IOException"/>
			public virtual T Deserialize(T @object)
			{
				try
				{
					// ignore passed-in object
					return (T)ois.ReadObject();
				}
				catch (TypeLoadException e)
				{
					throw new IOException(e.ToString());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				ois.Close();
			}
		}

		internal class JavaSerializationSerializer : Org.Apache.Hadoop.IO.Serializer.Serializer
			<Serializable>
		{
			private ObjectOutputStream oos;

			/// <exception cref="System.IO.IOException"/>
			public virtual void Open(OutputStream @out)
			{
				oos = new _ObjectOutputStream_79(@out);
			}

			private sealed class _ObjectOutputStream_79 : ObjectOutputStream
			{
				public _ObjectOutputStream_79(OutputStream baseArg1)
					: base(baseArg1)
				{
				}

				protected override void WriteStreamHeader()
				{
				}
			}

			// no header
			/// <exception cref="System.IO.IOException"/>
			public virtual void Serialize(Serializable @object)
			{
				oos.Reset();
				// clear (class) back-references
				oos.WriteObject(@object);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				oos.Close();
			}
		}

		[InterfaceAudience.Private]
		public virtual bool Accept(Type c)
		{
			return typeof(Serializable).IsAssignableFrom(c);
		}

		[InterfaceAudience.Private]
		public virtual Deserializer<Serializable> GetDeserializer(Type c)
		{
			return new JavaSerialization.JavaSerializationDeserializer<Serializable>();
		}

		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.IO.Serializer.Serializer<Serializable> GetSerializer
			(Type c)
		{
			return new JavaSerialization.JavaSerializationSerializer();
		}
	}
}
