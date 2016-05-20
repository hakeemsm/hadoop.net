using Sharpen;

namespace org.apache.hadoop.io.serializer.avro
{
	/// <summary>Base class for providing serialization to Avro types.</summary>
	public abstract class AvroSerialization<T> : org.apache.hadoop.conf.Configured, org.apache.hadoop.io.serializer.Serialization
		<T>
	{
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public const string AVRO_SCHEMA_KEY = "Avro-Schema";

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual org.apache.hadoop.io.serializer.Deserializer<T> getDeserializer(java.lang.Class
			 c)
		{
			return new org.apache.hadoop.io.serializer.avro.AvroSerialization.AvroDeserializer
				(this, c);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual org.apache.hadoop.io.serializer.Serializer<T> getSerializer(java.lang.Class
			 c)
		{
			return new org.apache.hadoop.io.serializer.avro.AvroSerialization.AvroSerializer(
				this, c);
		}

		/// <summary>Return an Avro Schema instance for the given class.</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public abstract org.apache.avro.Schema getSchema(T t);

		/// <summary>Create and return Avro DatumWriter for the given class.</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public abstract org.apache.avro.io.DatumWriter<T> getWriter(java.lang.Class clazz
			);

		/// <summary>Create and return Avro DatumReader for the given class.</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public abstract org.apache.avro.io.DatumReader<T> getReader(java.lang.Class clazz
			);

		internal class AvroSerializer : org.apache.hadoop.io.serializer.Serializer<T>
		{
			private org.apache.avro.io.DatumWriter<T> writer;

			private org.apache.avro.io.BinaryEncoder encoder;

			private java.io.OutputStream outStream;

			internal AvroSerializer(AvroSerialization<T> _enclosing, java.lang.Class clazz)
			{
				this._enclosing = _enclosing;
				this.writer = this._enclosing.getWriter(clazz);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				this.encoder.flush();
				this.outStream.close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void open(java.io.OutputStream @out)
			{
				this.outStream = @out;
				this.encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(@out, this.encoder
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void serialize(T t)
			{
				this.writer.setSchema(this._enclosing.getSchema(t));
				this.writer.write(t, this.encoder);
			}

			private readonly AvroSerialization<T> _enclosing;
		}

		internal class AvroDeserializer : org.apache.hadoop.io.serializer.Deserializer<T>
		{
			private org.apache.avro.io.DatumReader<T> reader;

			private org.apache.avro.io.BinaryDecoder decoder;

			private java.io.InputStream inStream;

			internal AvroDeserializer(AvroSerialization<T> _enclosing, java.lang.Class clazz)
			{
				this._enclosing = _enclosing;
				this.reader = this._enclosing.getReader(clazz);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				this.inStream.close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual T deserialize(T t)
			{
				return this.reader.read(t, this.decoder);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void open(java.io.InputStream @in)
			{
				this.inStream = @in;
				this.decoder = org.apache.avro.io.DecoderFactory.get().binaryDecoder(@in, this.decoder
					);
			}

			private readonly AvroSerialization<T> _enclosing;
		}

		public abstract bool accept(java.lang.Class arg1);
	}
}
