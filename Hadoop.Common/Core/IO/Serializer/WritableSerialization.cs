using System;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer
{
	/// <summary>
	/// A
	/// <see cref="Serialization{T}"/>
	/// for
	/// <see cref="Writable"/>
	/// s that delegates to
	/// <see cref="Writable.Write(System.IO.DataOutput)"/>
	/// and
	/// <see cref="Writable.ReadFields(System.IO.DataInput)"/>
	/// .
	/// </summary>
	public class WritableSerialization : Configured, Serialization<Writable>
	{
		internal class WritableDeserializer : Configured, Deserializer<Writable>
		{
			private Type writableClass;

			private DataInputStream dataIn;

			public WritableDeserializer(Configuration conf, Type c)
			{
				SetConf(conf);
				this.writableClass = c;
			}

			public virtual void Open(InputStream @in)
			{
				if (@in is DataInputStream)
				{
					dataIn = (DataInputStream)@in;
				}
				else
				{
					dataIn = new DataInputStream(@in);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Writable Deserialize(Writable w)
			{
				Writable writable;
				if (w == null)
				{
					writable = (Writable)ReflectionUtils.NewInstance(writableClass, GetConf());
				}
				else
				{
					writable = w;
				}
				writable.ReadFields(dataIn);
				return writable;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				dataIn.Close();
			}
		}

		internal class WritableSerializer : Configured, Org.Apache.Hadoop.IO.Serializer.Serializer
			<Writable>
		{
			private DataOutputStream dataOut;

			public virtual void Open(OutputStream @out)
			{
				if (@out is DataOutputStream)
				{
					dataOut = (DataOutputStream)@out;
				}
				else
				{
					dataOut = new DataOutputStream(@out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Serialize(Writable w)
			{
				w.Write(dataOut);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				dataOut.Close();
			}
		}

		[InterfaceAudience.Private]
		public virtual bool Accept(Type c)
		{
			return typeof(Writable).IsAssignableFrom(c);
		}

		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.IO.Serializer.Serializer<Writable> GetSerializer
			(Type c)
		{
			return new WritableSerialization.WritableSerializer();
		}

		[InterfaceAudience.Private]
		public virtual Deserializer<Writable> GetDeserializer(Type c)
		{
			return new WritableSerialization.WritableDeserializer(GetConf(), c);
		}
	}
}
