using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
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
	/// <see cref="IWritable"/>
	/// s that delegates to
	/// <see cref="IWritable.Write(System.IO.DataOutput)"/>
	/// and
	/// <see cref="IWritable.ReadFields(System.IO.BinaryReader)"/>
	/// .
	/// </summary>
	public class WritableSerialization : Configured, Serialization<IWritable>
	{
		internal class WritableDeserializer : Configured, Deserializer<IWritable>
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
			public virtual IWritable Deserialize(IWritable w)
			{
				IWritable writable;
				if (w == null)
				{
					writable = (IWritable)ReflectionUtils.NewInstance(writableClass, GetConf());
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
			<IWritable>
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
			public virtual void Serialize(IWritable w)
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
			return typeof(IWritable).IsAssignableFrom(c);
		}

		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.IO.Serializer.Serializer<IWritable> GetSerializer
			(Type c)
		{
			return new WritableSerialization.WritableSerializer();
		}

		[InterfaceAudience.Private]
		public virtual Deserializer<IWritable> GetDeserializer(Type c)
		{
			return new WritableSerialization.WritableDeserializer(GetConf(), c);
		}
	}
}
