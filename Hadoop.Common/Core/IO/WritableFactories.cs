using System;
using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Factories for non-public writables.</summary>
	/// <remarks>
	/// Factories for non-public writables.  Defining a factory permits
	/// <see cref="ObjectWritable"/>
	/// to be able to construct instances of non-public classes.
	/// </remarks>
	public class WritableFactories
	{
		private static readonly IDictionary<Type, WritableFactory> ClassToFactory = new ConcurrentHashMap
			<Type, WritableFactory>();

		private WritableFactories()
		{
		}

		// singleton
		/// <summary>Define a factory for a class.</summary>
		public static void SetFactory(Type c, WritableFactory factory)
		{
			ClassToFactory[c] = factory;
		}

		/// <summary>Define a factory for a class.</summary>
		public static WritableFactory GetFactory(Type c)
		{
			return ClassToFactory[c];
		}

		/// <summary>Create a new instance of a class with a defined factory.</summary>
		public static IWritable NewInstance(Type c, Configuration conf)
		{
			WritableFactory factory = Org.Apache.Hadoop.IO.WritableFactories.GetFactory(c);
			if (factory != null)
			{
				IWritable result = factory.NewInstance();
				if (result is Configurable)
				{
					((Configurable)result).SetConf(conf);
				}
				return result;
			}
			else
			{
				return ReflectionUtils.NewInstance(c, conf);
			}
		}

		/// <summary>Create a new instance of a class with a defined factory.</summary>
		public static IWritable NewInstance(Type c)
		{
			return NewInstance(c, null);
		}
	}
}
