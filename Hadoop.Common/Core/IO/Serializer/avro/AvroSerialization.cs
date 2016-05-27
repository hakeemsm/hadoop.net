using System;
using System.IO;
using Org.Apache.Avro;
using Org.Apache.Avro.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Serializer;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer.Avro
{
	/// <summary>Base class for providing serialization to Avro types.</summary>
	public abstract class AvroSerialization<T> : Configured, Serialization<T>
	{
		[InterfaceAudience.Private]
		public const string AvroSchemaKey = "Avro-Schema";

		[InterfaceAudience.Private]
		public virtual Deserializer<T> GetDeserializer(Type c)
		{
			return new AvroSerialization.AvroDeserializer(this, c);
		}

		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.IO.Serializer.Serializer<T> GetSerializer(Type c
			)
		{
			return new AvroSerialization.AvroSerializer(this, c);
		}

		/// <summary>Return an Avro Schema instance for the given class.</summary>
		[InterfaceAudience.Private]
		public abstract Schema GetSchema(T t);

		/// <summary>Create and return Avro DatumWriter for the given class.</summary>
		[InterfaceAudience.Private]
		public abstract DatumWriter<T> GetWriter(Type clazz);

		/// <summary>Create and return Avro DatumReader for the given class.</summary>
		[InterfaceAudience.Private]
		public abstract DatumReader<T> GetReader(Type clazz);

		internal class AvroSerializer : Org.Apache.Hadoop.IO.Serializer.Serializer<T>
		{
			private DatumWriter<T> writer;

			private BinaryEncoder encoder;

			private OutputStream outStream;

			internal AvroSerializer(AvroSerialization<T> _enclosing, Type clazz)
			{
				this._enclosing = _enclosing;
				this.writer = this._enclosing.GetWriter(clazz);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				this.encoder.Flush();
				this.outStream.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Open(OutputStream @out)
			{
				this.outStream = @out;
				this.encoder = EncoderFactory.Get().BinaryEncoder(@out, this.encoder);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Serialize(T t)
			{
				this.writer.SetSchema(this._enclosing.GetSchema(t));
				this.writer.Write(t, this.encoder);
			}

			private readonly AvroSerialization<T> _enclosing;
		}

		internal class AvroDeserializer : Deserializer<T>
		{
			private DatumReader<T> reader;

			private BinaryDecoder decoder;

			private InputStream inStream;

			internal AvroDeserializer(AvroSerialization<T> _enclosing, Type clazz)
			{
				this._enclosing = _enclosing;
				this.reader = this._enclosing.GetReader(clazz);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				this.inStream.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual T Deserialize(T t)
			{
				return this.reader.Read(t, this.decoder);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Open(InputStream @in)
			{
				this.inStream = @in;
				this.decoder = DecoderFactory.Get().BinaryDecoder(@in, this.decoder);
			}

			private readonly AvroSerialization<T> _enclosing;
		}

		public abstract bool Accept(Type arg1);
	}
}
