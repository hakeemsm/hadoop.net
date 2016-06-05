using System;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// DefaultStringifier is the default implementation of the
	/// <see cref="Stringifier{T}"/>
	/// interface which stringifies the objects using base64 encoding of the
	/// serialized version of the objects. The
	/// <see cref="Org.Apache.Hadoop.IO.Serializer.Serializer{T}"/>
	/// and
	/// <see cref="Org.Apache.Hadoop.IO.Serializer.Deserializer{T}"/>
	/// are obtained from the
	/// <see cref="Org.Apache.Hadoop.IO.Serializer.SerializationFactory"/>
	/// .
	/// <br />
	/// DefaultStringifier offers convenience methods to store/load objects to/from
	/// the configuration.
	/// </summary>
	/// <?/>
	public class DefaultStringifier<T> : Stringifier<T>
	{
		private const string Separator = ",";

		private Org.Apache.Hadoop.IO.Serializer.Serializer<T> serializer;

		private Deserializer<T> deserializer;

		private DataInputBuffer inBuf;

		private DataOutputBuffer outBuf;

		public DefaultStringifier(Configuration conf, Type c)
		{
			SerializationFactory factory = new SerializationFactory(conf);
			this.serializer = factory.GetSerializer(c);
			this.deserializer = factory.GetDeserializer(c);
			this.inBuf = new DataInputBuffer();
			this.outBuf = new DataOutputBuffer();
			try
			{
				serializer.Open(outBuf);
				deserializer.Open(inBuf);
			}
			catch (IOException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual T FromString(string str)
		{
			try
			{
				byte[] bytes = Base64.DecodeBase64(Runtime.GetBytesForString(str, "UTF-8"
					));
				inBuf.Reset(bytes, bytes.Length);
				T restored = deserializer.Deserialize(null);
				return restored;
			}
			catch (UnsupportedCharsetException ex)
			{
				throw new IOException(ex.ToString());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string ToString(T obj)
		{
			outBuf.Reset();
			serializer.Serialize(obj);
			byte[] buf = new byte[outBuf.GetLength()];
			System.Array.Copy(outBuf.GetData(), 0, buf, 0, buf.Length);
			return new string(Base64.EncodeBase64(buf), Charsets.Utf8);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			inBuf.Close();
			outBuf.Close();
			deserializer.Close();
			serializer.Close();
		}

		/// <summary>Stores the item in the configuration with the given keyName.</summary>
		/// <?/>
		/// <param name="conf">the configuration to store</param>
		/// <param name="item">the object to be stored</param>
		/// <param name="keyName">the name of the key to use</param>
		/// <exception cref="System.IO.IOException">
		/// : forwards Exceptions from the underlying
		/// <see cref="Org.Apache.Hadoop.IO.Serializer.Serialization{T}"/>
		/// classes.
		/// </exception>
		public static void Store<K>(Configuration conf, K item, string keyName)
		{
			Org.Apache.Hadoop.IO.DefaultStringifier<K> stringifier = new Org.Apache.Hadoop.IO.DefaultStringifier
				<K>(conf, GenericsUtil.GetClass(item));
			conf.Set(keyName, stringifier.ToString(item));
			stringifier.Close();
		}

		/// <summary>Restores the object from the configuration.</summary>
		/// <?/>
		/// <param name="conf">the configuration to use</param>
		/// <param name="keyName">the name of the key to use</param>
		/// <param name="itemClass">the class of the item</param>
		/// <returns>restored object</returns>
		/// <exception cref="System.IO.IOException">
		/// : forwards Exceptions from the underlying
		/// <see cref="Org.Apache.Hadoop.IO.Serializer.Serialization{T}"/>
		/// classes.
		/// </exception>
		public static K Load<K>(Configuration conf, string keyName)
		{
			System.Type itemClass = typeof(K);
			Org.Apache.Hadoop.IO.DefaultStringifier<K> stringifier = new Org.Apache.Hadoop.IO.DefaultStringifier
				<K>(conf, itemClass);
			try
			{
				string itemStr = conf.Get(keyName);
				return stringifier.FromString(itemStr);
			}
			finally
			{
				stringifier.Close();
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
		/// <see cref="Org.Apache.Hadoop.IO.Serializer.Serialization{T}"/>
		/// classes.
		/// </exception>
		public static void StoreArray<K>(Configuration conf, K[] items, string keyName)
		{
			Org.Apache.Hadoop.IO.DefaultStringifier<K> stringifier = new Org.Apache.Hadoop.IO.DefaultStringifier
				<K>(conf, GenericsUtil.GetClass(items[0]));
			try
			{
				StringBuilder builder = new StringBuilder();
				foreach (K item in items)
				{
					builder.Append(stringifier.ToString(item)).Append(Separator);
				}
				conf.Set(keyName, builder.ToString());
			}
			finally
			{
				stringifier.Close();
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
		/// <see cref="Org.Apache.Hadoop.IO.Serializer.Serialization{T}"/>
		/// classes.
		/// </exception>
		public static K[] LoadArray<K>(Configuration conf, string keyName)
		{
			System.Type itemClass = typeof(K);
			Org.Apache.Hadoop.IO.DefaultStringifier<K> stringifier = new Org.Apache.Hadoop.IO.DefaultStringifier
				<K>(conf, itemClass);
			try
			{
				string itemStr = conf.Get(keyName);
				AList<K> list = new AList<K>();
				string[] parts = itemStr.Split(Separator);
				foreach (string part in parts)
				{
					if (!part.IsEmpty())
					{
						list.AddItem(stringifier.FromString(part));
					}
				}
				return GenericsUtil.ToArray(itemClass, list);
			}
			finally
			{
				stringifier.Close();
			}
		}
	}
}
