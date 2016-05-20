using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	/// <summary>
	/// <p>
	/// A factory for
	/// <see cref="Serialization{T}"/>
	/// s.
	/// </p>
	/// </summary>
	public class SerializationFactory : org.apache.hadoop.conf.Configured
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.serializer.SerializationFactory
			)).getName());

		private System.Collections.Generic.IList<org.apache.hadoop.io.serializer.Serialization
			<object>> serializations = new System.Collections.Generic.List<org.apache.hadoop.io.serializer.Serialization
			<object>>();

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
		public SerializationFactory(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
			foreach (string serializerName in conf.getTrimmedStrings(org.apache.hadoop.fs.CommonConfigurationKeys
				.IO_SERIALIZATIONS_KEY, new string[] { Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.serializer.WritableSerialization
				)).getName(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization
				)).getName(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.serializer.avro.AvroReflectSerialization
				)).getName() }))
			{
				add(conf, serializerName);
			}
		}

		private void add(org.apache.hadoop.conf.Configuration conf, string serializationName
			)
		{
			try
			{
				java.lang.Class serializionClass = (java.lang.Class)conf.getClassByName(serializationName
					);
				serializations.add((org.apache.hadoop.io.serializer.Serialization)org.apache.hadoop.util.ReflectionUtils
					.newInstance(serializionClass, getConf()));
			}
			catch (java.lang.ClassNotFoundException e)
			{
				LOG.warn("Serialization class not found: ", e);
			}
		}

		public virtual org.apache.hadoop.io.serializer.Serializer<T> getSerializer<T>()
		{
			System.Type c = typeof(T);
			org.apache.hadoop.io.serializer.Serialization<T> serializer = getSerialization(c);
			if (serializer != null)
			{
				return serializer.getSerializer(c);
			}
			return null;
		}

		public virtual org.apache.hadoop.io.serializer.Deserializer<T> getDeserializer<T>
			()
		{
			System.Type c = typeof(T);
			org.apache.hadoop.io.serializer.Serialization<T> serializer = getSerialization(c);
			if (serializer != null)
			{
				return serializer.getDeserializer(c);
			}
			return null;
		}

		public virtual org.apache.hadoop.io.serializer.Serialization<T> getSerialization<
			T>()
		{
			System.Type c = typeof(T);
			foreach (org.apache.hadoop.io.serializer.Serialization serialization in serializations)
			{
				if (serialization.accept(c))
				{
					return (org.apache.hadoop.io.serializer.Serialization<T>)serialization;
				}
			}
			return null;
		}
	}
}
