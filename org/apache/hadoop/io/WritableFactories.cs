using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Factories for non-public writables.</summary>
	/// <remarks>
	/// Factories for non-public writables.  Defining a factory permits
	/// <see cref="ObjectWritable"/>
	/// to be able to construct instances of non-public classes.
	/// </remarks>
	public class WritableFactories
	{
		private static readonly System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.io.WritableFactory
			> CLASS_TO_FACTORY = new java.util.concurrent.ConcurrentHashMap<java.lang.Class, 
			org.apache.hadoop.io.WritableFactory>();

		private WritableFactories()
		{
		}

		// singleton
		/// <summary>Define a factory for a class.</summary>
		public static void setFactory(java.lang.Class c, org.apache.hadoop.io.WritableFactory
			 factory)
		{
			CLASS_TO_FACTORY[c] = factory;
		}

		/// <summary>Define a factory for a class.</summary>
		public static org.apache.hadoop.io.WritableFactory getFactory(java.lang.Class c)
		{
			return CLASS_TO_FACTORY[c];
		}

		/// <summary>Create a new instance of a class with a defined factory.</summary>
		public static org.apache.hadoop.io.Writable newInstance(java.lang.Class c, org.apache.hadoop.conf.Configuration
			 conf)
		{
			org.apache.hadoop.io.WritableFactory factory = org.apache.hadoop.io.WritableFactories
				.getFactory(c);
			if (factory != null)
			{
				org.apache.hadoop.io.Writable result = factory.newInstance();
				if (result is org.apache.hadoop.conf.Configurable)
				{
					((org.apache.hadoop.conf.Configurable)result).setConf(conf);
				}
				return result;
			}
			else
			{
				return org.apache.hadoop.util.ReflectionUtils.newInstance(c, conf);
			}
		}

		/// <summary>Create a new instance of a class with a defined factory.</summary>
		public static org.apache.hadoop.io.Writable newInstance(java.lang.Class c)
		{
			return newInstance(c, null);
		}
	}
}
