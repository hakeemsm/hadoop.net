using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>Created 21-Jan-2009 13:42:36</summary>
	public class TestConfigurationSubclass : NUnit.Framework.TestCase
	{
		private const string EMPTY_CONFIGURATION_XML = "/org/apache/hadoop/conf/empty-configuration.xml";

		public virtual void testGetProps()
		{
			org.apache.hadoop.conf.TestConfigurationSubclass.SubConf conf = new org.apache.hadoop.conf.TestConfigurationSubclass.SubConf
				(true);
			java.util.Properties properties = conf.getProperties();
			NUnit.Framework.Assert.IsNotNull("hadoop.tmp.dir is not set", properties.getProperty
				("hadoop.tmp.dir"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testReload()
		{
			org.apache.hadoop.conf.TestConfigurationSubclass.SubConf conf = new org.apache.hadoop.conf.TestConfigurationSubclass.SubConf
				(true);
			NUnit.Framework.Assert.IsFalse(conf.isReloaded());
			org.apache.hadoop.conf.Configuration.addDefaultResource(EMPTY_CONFIGURATION_XML);
			NUnit.Framework.Assert.IsTrue(conf.isReloaded());
			java.util.Properties properties = conf.getProperties();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testReloadNotQuiet()
		{
			org.apache.hadoop.conf.TestConfigurationSubclass.SubConf conf = new org.apache.hadoop.conf.TestConfigurationSubclass.SubConf
				(true);
			conf.setQuietMode(false);
			NUnit.Framework.Assert.IsFalse(conf.isReloaded());
			conf.addResource("not-a-valid-resource");
			NUnit.Framework.Assert.IsTrue(conf.isReloaded());
			try
			{
				java.util.Properties properties = conf.getProperties();
				fail("Should not have got here");
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e.ToString(), e.Message.contains("not found"));
			}
		}

		private class SubConf : org.apache.hadoop.conf.Configuration
		{
			private bool reloaded;

			/// <summary>
			/// A new configuration where the behavior of reading from the default resources
			/// can be turned off.
			/// </summary>
			/// <remarks>
			/// A new configuration where the behavior of reading from the default resources
			/// can be turned off.
			/// If the parameter
			/// <paramref name="loadDefaults"/>
			/// is false, the new instance will not
			/// load resources from the default files.
			/// </remarks>
			/// <param name="loadDefaults">specifies whether to load from the default files</param>
			private SubConf(bool loadDefaults)
				: base(loadDefaults)
			{
			}

			public virtual java.util.Properties getProperties()
			{
				return base.getProps();
			}

			/// <summary>
			/// <inheritDoc/>
			/// .
			/// Sets the reloaded flag.
			/// </summary>
			public override void reloadConfiguration()
			{
				base.reloadConfiguration();
				reloaded = true;
			}

			public virtual bool isReloaded()
			{
				return reloaded;
			}

			public virtual void setReloaded(bool reloaded)
			{
				this.reloaded = reloaded;
			}
		}
	}
}
