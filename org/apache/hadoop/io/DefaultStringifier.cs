using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// DefaultStringifier is the default implementation of the
	/// <see cref="Stringifier{T}"/>
	/// interface which stringifies the objects using base64 encoding of the
	/// serialized version of the objects. The
	/// <see cref="org.apache.hadoop.io.serializer.Serializer{T}"/>
	/// and
	/// <see cref="org.apache.hadoop.io.serializer.Deserializer{T}"/>
	/// are obtained from the
	/// <see cref="org.apache.hadoop.io.serializer.SerializationFactory"/>
	/// .
	/// <br />
	/// DefaultStringifier offers convenience methods to store/load objects to/from
	/// the configuration.
	/// </summary>
	/// <?/>
	public class DefaultStringifier<T> : org.apache.hadoop.io.Stringifier<T>
	{
		private const string SEPARATOR = ",";

		private org.apache.hadoop.io.serializer.Serializer<T> serializer;

		private org.apache.hadoop.io.serializer.Deserializer<T> deserializer;

		private org.apache.hadoop.io.DataInputBuffer inBuf;

		private org.apache.hadoop.io.DataOutputBuffer outBuf;

		public DefaultStringifier(org.apache.hadoop.conf.Configuration conf, java.lang.Class
			 c)
		{
			org.apache.hadoop.io.serializer.SerializationFactory factory = new org.apache.hadoop.io.serializer.SerializationFactory
				(conf);
			this.serializer = factory.getSerializer(c);
			this.deserializer = factory.getDeserializer(c);
			this.inBuf = new org.apache.hadoop.io.DataInputBuffer();
			this.outBuf = new org.apache.hadoop.io.DataOutputBuffer();
			try
			{
				serializer.open(outBuf);
				deserializer.open(inBuf);
			}
			catch (System.IO.IOException ex)
			{
				throw new System.Exception(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual T fromString(string str)
		{
			try
			{
				byte[] bytes = org.apache.commons.codec.binary.Base64.decodeBase64(Sharpen.Runtime.getBytesForString
					(str, "UTF-8"));
				inBuf.reset(bytes, bytes.Length);
				T restored = deserializer.deserialize(null);
				return restored;
			}
			catch (java.nio.charset.UnsupportedCharsetException ex)
			{
				throw new System.IO.IOException(ex.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string toString(T obj)
		{
			outBuf.reset();
			serializer.serialize(obj);
			byte[] buf = new byte[outBuf.getLength()];
			System.Array.Copy(outBuf.getData(), 0, buf, 0, buf.Length);
			return new string(org.apache.commons.codec.binary.Base64.encodeBase64(buf), org.apache.commons.io.Charsets
				.UTF_8);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			inBuf.close();
			outBuf.close();
			deserializer.close();
			serializer.close();
		}

		/// <summary>Stores the item in the configuration with the given keyName.</summary>
		/// <?/>
		/// <param name="conf">the configuration to store</param>
		/// <param name="item">the object to be stored</param>
		/// <param name="keyName">the name of the key to use</param>
		/// <exception cref="System.IO.IOException">
		/// : forwards Exceptions from the underlying
		/// <see cref="org.apache.hadoop.io.serializer.Serialization{T}"/>
		/// classes.
		/// </exception>
		public static void store<K>(org.apache.hadoop.conf.Configuration conf, K item, string
			 keyName)
		{
			org.apache.hadoop.io.DefaultStringifier<K> stringifier = new org.apache.hadoop.io.DefaultStringifier
				<K>(conf, org.apache.hadoop.util.GenericsUtil.getClass(item));
			conf.set(keyName, stringifier.toString(item));
			stringifier.close();
		}

		/// <summary>Restores the object from the configuration.</summary>
		/// <?/>
		/// <param name="conf">the configuration to use</param>
		/// <param name="keyName">the name of the key to use</param>
		/// <param name="itemClass">the class of the item</param>
		/// <returns>restored object</returns>
		/// <exception cref="System.IO.IOException">
		/// : forwards Exceptions from the underlying
		/// <see cref="org.apache.hadoop.io.serializer.Serialization{T}"/>
		/// classes.
		/// </exception>
		public static K load<K>(org.apache.hadoop.conf.Configuration conf, string keyName
			)
		{
			System.Type itemClass = typeof(K);
			org.apache.hadoop.io.DefaultStringifier<K> stringifier = new org.apache.hadoop.io.DefaultStringifier
				<K>(conf, itemClass);
			try
			{
				string itemStr = conf.get(keyName);
				return stringifier.fromString(itemStr);
			}
			finally
			{
				stringifier.close();
			}
		}

		/// <summary>Stores the array of items in the configuration with the given keyName.</summary>
		/// <?/>
		/// <param name="conf">the configuration to use</param>
		/// <param name="items">the objects to be stored</param>
		/// <param name="keyName">the name of the key to use</param>
		/// <exception cref="System.IndexOutOfRangeException">if the items array is empty</exception>
		/// <exception cref="System.IO.IOException">
		/// : forwards Exceptions from the underlying
		/// <see cref="org.apache.hadoop.io.serializer.Serialization{T}"/>
		/// classes.
		/// </exception>
		public static void storeArray<K>(org.apache.hadoop.conf.Configuration conf, K[] items
			, string keyName)
		{
			org.apache.hadoop.io.DefaultStringifier<K> stringifier = new org.apache.hadoop.io.DefaultStringifier
				<K>(conf, org.apache.hadoop.util.GenericsUtil.getClass(items[0]));
			try
			{
				java.lang.StringBuilder builder = new java.lang.StringBuilder();
				foreach (K item in items)
				{
					builder.Append(stringifier.toString(item)).Append(SEPARATOR);
				}
				conf.set(keyName, builder.ToString());
			}
			finally
			{
				stringifier.close();
			}
		}

		/// <summary>Restores the array of objects from the configuration.</summary>
		/// <?/>
		/// <param name="conf">the configuration to use</param>
		/// <param name="keyName">the name of the key to use</param>
		/// <param name="itemClass">the class of the item</param>
		/// <returns>restored object</returns>
		/// <exception cref="System.IO.IOException">
		/// : forwards Exceptions from the underlying
		/// <see cref="org.apache.hadoop.io.serializer.Serialization{T}"/>
		/// classes.
		/// </exception>
		public static K[] loadArray<K>(org.apache.hadoop.conf.Configuration conf, string 
			keyName)
		{
			System.Type itemClass = typeof(K);
			org.apache.hadoop.io.DefaultStringifier<K> stringifier = new org.apache.hadoop.io.DefaultStringifier
				<K>(conf, itemClass);
			try
			{
				string itemStr = conf.get(keyName);
				System.Collections.Generic.List<K> list = new System.Collections.Generic.List<K>(
					);
				string[] parts = itemStr.split(SEPARATOR);
				foreach (string part in parts)
				{
					if (!part.isEmpty())
					{
						list.add(stringifier.fromString(part));
					}
				}
				return org.apache.hadoop.util.GenericsUtil.toArray(itemClass, list);
			}
			finally
			{
				stringifier.close();
			}
		}
	}
}
