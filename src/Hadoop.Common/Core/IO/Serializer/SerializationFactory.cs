using System;
using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Serializer.Avro;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO.Serializer
{
	/// <summary>
	/// <p>
	/// A factory for
	/// <see cref="Serialization{T}"/>
	/// s.
	/// </p>
	/// </summary>
	public class SerializationFactory : Configured
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.Serializer.SerializationFactory
			).FullName);

		private IList<Serialization<object>> serializations = new AList<Serialization<object
			>>();

		/// <summary>
		/// <p>
		/// Serializations are found by reading the <code>io.serializations</code>
		/// property from <code>conf</code>, which is a comma-delimited list of
		/// classnames.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Serializations are found by reading the <code>io.serializations</code>
		/// property from <code>conf</code>, which is a comma-delimited list of
		/// classnames.
		/// </p>
		/// </remarks>
		public SerializationFactory(Configuration conf)
			: base(conf)
		{
			foreach (string serializerName in conf.GetTrimmedStrings(CommonConfigurationKeys.
				IoSerializationsKey, new string[] { typeof(WritableSerialization).FullName, typeof(
				AvroSpecificSerialization).FullName, typeof(AvroReflectSerialization).FullName }
				))
			{
				Add(conf, serializerName);
			}
		}

		private void Add(Configuration conf, string serializationName)
		{
			try
			{
				Type serializionClass = (Type)conf.GetClassByName(serializationName);
				serializations.AddItem((Serialization)ReflectionUtils.NewInstance(serializionClass
					, GetConf()));
			}
			catch (TypeLoadException e)
			{
				Log.Warn("Serialization class not found: ", e);
			}
		}

		public virtual Org.Apache.Hadoop.IO.Serializer.Serializer<T> GetSerializer<T>()
		{
			System.Type c = typeof(T);
			Serialization<T> serializer = GetSerialization(c);
			if (serializer != null)
			{
				return serializer.GetSerializer(c);
			}
			return null;
		}

		public virtual Deserializer<T> GetDeserializer<T>()
		{
			System.Type c = typeof(T);
			Serialization<T> serializer = GetSerialization(c);
			if (serializer != null)
			{
				return serializer.GetDeserializer(c);
			}
			return null;
		}

		public virtual Serialization<T> GetSerialization<T>()
		{
			System.Type c = typeof(T);
			foreach (Serialization serialization in serializations)
			{
				if (serialization.Accept(c))
				{
					return (Serialization<T>)serialization;
				}
			}
			return null;
		}
	}
}
