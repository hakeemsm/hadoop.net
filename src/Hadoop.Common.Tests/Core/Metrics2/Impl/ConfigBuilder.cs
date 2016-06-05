using System;
using Org.Apache.Commons.Configuration;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Helper class for building configs, mostly used in tests</summary>
	public class ConfigBuilder
	{
		/// <summary>The built config</summary>
		public readonly PropertiesConfiguration config;

		/// <summary>Default constructor</summary>
		public ConfigBuilder()
		{
			config = new PropertiesConfiguration();
		}

		/// <summary>Add a property to the config</summary>
		/// <param name="key">of the property</param>
		/// <param name="value">of the property</param>
		/// <returns>self</returns>
		public virtual Org.Apache.Hadoop.Metrics2.Impl.ConfigBuilder Add(string key, object
			 value)
		{
			config.AddProperty(key, value);
			return this;
		}

		/// <summary>Save the config to a file</summary>
		/// <param name="filename">to save</param>
		/// <returns>self</returns>
		/// <exception cref="RuntimeException"/>
		public virtual Org.Apache.Hadoop.Metrics2.Impl.ConfigBuilder Save(string filename
			)
		{
			try
			{
				config.Save(filename);
			}
			catch (Exception e)
			{
				throw new RuntimeException("Error saving config", e);
			}
			return this;
		}

		/// <summary>Return a subset configuration (so getParent() can be used.)</summary>
		/// <param name="prefix">of the subset</param>
		/// <returns>the subset config</returns>
		public virtual SubsetConfiguration Subset(string prefix)
		{
			return new SubsetConfiguration(config, prefix, ".");
		}
	}
}
