using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	/// <summary>
	/// A
	/// <see cref="Serialization{T}"/>
	/// for
	/// <see cref="org.apache.hadoop.io.Writable"/>
	/// s that delegates to
	/// <see cref="org.apache.hadoop.io.Writable.write(java.io.DataOutput)"/>
	/// and
	/// <see cref="org.apache.hadoop.io.Writable.readFields(java.io.DataInput)"/>
	/// .
	/// </summary>
	public class WritableSerialization : org.apache.hadoop.conf.Configured, org.apache.hadoop.io.serializer.Serialization
		<org.apache.hadoop.io.Writable>
	{
		internal class WritableDeserializer : org.apache.hadoop.conf.Configured, org.apache.hadoop.io.serializer.Deserializer
			<org.apache.hadoop.io.Writable>
		{
			private java.lang.Class writableClass;

			private java.io.DataInputStream dataIn;

			public WritableDeserializer(org.apache.hadoop.conf.Configuration conf, java.lang.Class
				 c)
			{
				setConf(conf);
				this.writableClass = c;
			}

			public virtual void open(java.io.InputStream @in)
			{
				if (@in is java.io.DataInputStream)
				{
					dataIn = (java.io.DataInputStream)@in;
				}
				else
				{
					dataIn = new java.io.DataInputStream(@in);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.Writable deserialize(org.apache.hadoop.io.Writable
				 w)
			{
				org.apache.hadoop.io.Writable writable;
				if (w == null)
				{
					writable = (org.apache.hadoop.io.Writable)org.apache.hadoop.util.ReflectionUtils.
						newInstance(writableClass, getConf());
				}
				else
				{
					writable = w;
				}
				writable.readFields(dataIn);
				return writable;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				dataIn.close();
			}
		}

		internal class WritableSerializer : org.apache.hadoop.conf.Configured, org.apache.hadoop.io.serializer.Serializer
			<org.apache.hadoop.io.Writable>
		{
			private java.io.DataOutputStream dataOut;

			public virtual void open(java.io.OutputStream @out)
			{
				if (@out is java.io.DataOutputStream)
				{
					dataOut = (java.io.DataOutputStream)@out;
				}
				else
				{
					dataOut = new java.io.DataOutputStream(@out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void serialize(org.apache.hadoop.io.Writable w)
			{
				w.write(dataOut);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				dataOut.close();
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual bool accept(java.lang.Class c)
		{
			return Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Writable)).isAssignableFrom
				(c);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual org.apache.hadoop.io.serializer.Serializer<org.apache.hadoop.io.Writable
			> getSerializer(java.lang.Class c)
		{
			return new org.apache.hadoop.io.serializer.WritableSerialization.WritableSerializer
				();
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual org.apache.hadoop.io.serializer.Deserializer<org.apache.hadoop.io.Writable
			> getDeserializer(java.lang.Class c)
		{
			return new org.apache.hadoop.io.serializer.WritableSerialization.WritableDeserializer
				(getConf(), c);
		}
	}
}
