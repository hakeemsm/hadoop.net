using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	/// <summary>Created 21-Jan-2009 13:42:36</summary>
	public class TestConfigurationSubclass : TestCase
	{
		private const string EmptyConfigurationXml = "/org/apache/hadoop/conf/empty-configuration.xml";

		public virtual void TestGetProps()
		{
			TestConfigurationSubclass.SubConf conf = new TestConfigurationSubclass.SubConf(true
				);
			Properties properties = conf.GetProperties();
			NUnit.Framework.Assert.IsNotNull("hadoop.tmp.dir is not set", properties.GetProperty
				("hadoop.tmp.dir"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReload()
		{
			TestConfigurationSubclass.SubConf conf = new TestConfigurationSubclass.SubConf(true
				);
			NUnit.Framework.Assert.IsFalse(conf.IsReloaded());
			Configuration.AddDefaultResource(EmptyConfigurationXml);
			NUnit.Framework.Assert.IsTrue(conf.IsReloaded());
			Properties properties = conf.GetProperties();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReloadNotQuiet()
		{
			TestConfigurationSubclass.SubConf conf = new TestConfigurationSubclass.SubConf(true
				);
			conf.SetQuietMode(false);
			NUnit.Framework.Assert.IsFalse(conf.IsReloaded());
			conf.AddResource("not-a-valid-resource");
			NUnit.Framework.Assert.IsTrue(conf.IsReloaded());
			try
			{
				Properties properties = conf.GetProperties();
				Fail("Should not have got here");
			}
			catch (RuntimeException e)
			{
				NUnit.Framework.Assert.IsTrue(e.ToString(), e.Message.Contains("not found"));
			}
		}

		private class SubConf : Configuration
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

			public virtual Properties GetProperties()
			{
				return base.GetProps();
			}

			/// <summary>
			/// <inheritDoc/>
			/// .
			/// Sets the reloaded flag.
			/// </summary>
			public override void ReloadConfiguration()
			{
				base.ReloadConfiguration();
				reloaded = true;
			}

			public virtual bool IsReloaded()
			{
				return reloaded;
			}

			public virtual void SetReloaded(bool reloaded)
			{
				this.reloaded = reloaded;
			}
		}
	}
}
