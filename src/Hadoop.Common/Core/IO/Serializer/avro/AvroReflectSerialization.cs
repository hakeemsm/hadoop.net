using System;
using System.Collections.Generic;
using Org.Apache.Avro;
using Org.Apache.Avro.IO;
using Org.Apache.Avro.Reflect;
using Org.Apache.Hadoop.Classification;


namespace Org.Apache.Hadoop.IO.Serializer.Avro
{
	/// <summary>Serialization for Avro Reflect classes.</summary>
	/// <remarks>
	/// Serialization for Avro Reflect classes. For a class to be accepted by this
	/// serialization, it must either be in the package list configured via
	/// <code>avro.reflect.pkgs</code> or implement
	/// <see cref="AvroReflectSerializable"/>
	/// interface.
	/// </remarks>
	public class AvroReflectSerialization : AvroSerialization<object>
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
		[InterfaceAudience.Private]
		public const string AvroReflectPackages = "avro.reflect.pkgs";

		private ICollection<string> packages;

		[InterfaceAudience.Private]
		public override bool Accept(Type c)
		{
			lock (this)
			{
				if (packages == null)
				{
					GetPackages();
				}
				return typeof(AvroReflectSerializable).IsAssignableFrom(c) || (c.Assembly != null
					 && packages.Contains(c.Assembly.GetName()));
			}
		}

		private void GetPackages()
		{
			string[] pkgList = GetConf().GetStrings(AvroReflectPackages);
			packages = new HashSet<string>();
			if (pkgList != null)
			{
				foreach (string pkg in pkgList)
				{
					packages.AddItem(pkg.Trim());
				}
			}
		}

		[InterfaceAudience.Private]
		public override DatumReader<object> GetReader(Type clazz)
		{
			try
			{
				return new ReflectDatumReader(clazz);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		[InterfaceAudience.Private]
		public override Schema GetSchema(object t)
		{
			return ((ReflectData)ReflectData.Get()).GetSchema(t.GetType());
		}

		[InterfaceAudience.Private]
		public override DatumWriter<object> GetWriter(Type clazz)
		{
			return new ReflectDatumWriter();
		}
	}
}
