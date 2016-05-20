using Sharpen;

namespace org.apache.hadoop.io.serializer.avro
{
	/// <summary>Serialization for Avro Reflect classes.</summary>
	/// <remarks>
	/// Serialization for Avro Reflect classes. For a class to be accepted by this
	/// serialization, it must either be in the package list configured via
	/// <code>avro.reflect.pkgs</code> or implement
	/// <see cref="AvroReflectSerializable"/>
	/// interface.
	/// </remarks>
	public class AvroReflectSerialization : org.apache.hadoop.io.serializer.avro.AvroSerialization
		<object>
	{
		/// <summary>
		/// Key to configure packages that contain classes to be serialized and
		/// deserialized using this class.
		/// </summary>
		/// <remarks>
		/// Key to configure packages that contain classes to be serialized and
		/// deserialized using this class. Multiple packages can be specified using
		/// comma-separated list.
		/// </remarks>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public const string AVRO_REFLECT_PACKAGES = "avro.reflect.pkgs";

		private System.Collections.Generic.ICollection<string> packages;

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override bool accept(java.lang.Class c)
		{
			lock (this)
			{
				if (packages == null)
				{
					getPackages();
				}
				return Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.serializer.avro.AvroReflectSerializable
					)).isAssignableFrom(c) || (c.getPackage() != null && packages.contains(c.getPackage
					().getName()));
			}
		}

		private void getPackages()
		{
			string[] pkgList = getConf().getStrings(AVRO_REFLECT_PACKAGES);
			packages = new java.util.HashSet<string>();
			if (pkgList != null)
			{
				foreach (string pkg in pkgList)
				{
					packages.add(pkg.Trim());
				}
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override org.apache.avro.io.DatumReader<object> getReader(java.lang.Class 
			clazz)
		{
			try
			{
				return new org.apache.avro.reflect.ReflectDatumReader(clazz);
			}
			catch (System.Exception e)
			{
				throw new System.Exception(e);
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override org.apache.avro.Schema getSchema(object t)
		{
			return ((org.apache.avro.reflect.ReflectData)org.apache.avro.reflect.ReflectData.
				get()).getSchema(Sharpen.Runtime.getClassForObject(t));
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override org.apache.avro.io.DatumWriter<object> getWriter(java.lang.Class 
			clazz)
		{
			return new org.apache.avro.reflect.ReflectDatumWriter();
		}
	}
}
