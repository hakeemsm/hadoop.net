using System.IO;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Util
{
	public class TestConfigurationUtils
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Constructors()
		{
			Configuration conf = new Configuration(false);
			NUnit.Framework.Assert.AreEqual(conf.Size(), 0);
			byte[] bytes = Sharpen.Runtime.GetBytesForString("<configuration><property><name>a</name><value>A</value></property></configuration>"
				);
			InputStream @is = new ByteArrayInputStream(bytes);
			conf = new Configuration(false);
			ConfigurationUtils.Load(conf, @is);
			NUnit.Framework.Assert.AreEqual(conf.Size(), 1);
			NUnit.Framework.Assert.AreEqual(conf.Get("a"), "A");
		}

		/// <exception cref="System.Exception"/>
		public virtual void ConstructorsFail3()
		{
			InputStream @is = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString("<xonfiguration></xonfiguration>"
				));
			Configuration conf = new Configuration(false);
			ConfigurationUtils.Load(conf, @is);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Copy()
		{
			Configuration srcConf = new Configuration(false);
			Configuration targetConf = new Configuration(false);
			srcConf.Set("testParameter1", "valueFromSource");
			srcConf.Set("testParameter2", "valueFromSource");
			targetConf.Set("testParameter2", "valueFromTarget");
			targetConf.Set("testParameter3", "valueFromTarget");
			ConfigurationUtils.Copy(srcConf, targetConf);
			NUnit.Framework.Assert.AreEqual("valueFromSource", targetConf.Get("testParameter1"
				));
			NUnit.Framework.Assert.AreEqual("valueFromSource", targetConf.Get("testParameter2"
				));
			NUnit.Framework.Assert.AreEqual("valueFromTarget", targetConf.Get("testParameter3"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void InjectDefaults()
		{
			Configuration srcConf = new Configuration(false);
			Configuration targetConf = new Configuration(false);
			srcConf.Set("testParameter1", "valueFromSource");
			srcConf.Set("testParameter2", "valueFromSource");
			targetConf.Set("testParameter2", "originalValueFromTarget");
			targetConf.Set("testParameter3", "originalValueFromTarget");
			ConfigurationUtils.InjectDefaults(srcConf, targetConf);
			NUnit.Framework.Assert.AreEqual("valueFromSource", targetConf.Get("testParameter1"
				));
			NUnit.Framework.Assert.AreEqual("originalValueFromTarget", targetConf.Get("testParameter2"
				));
			NUnit.Framework.Assert.AreEqual("originalValueFromTarget", targetConf.Get("testParameter3"
				));
			NUnit.Framework.Assert.AreEqual("valueFromSource", srcConf.Get("testParameter1"));
			NUnit.Framework.Assert.AreEqual("valueFromSource", srcConf.Get("testParameter2"));
			NUnit.Framework.Assert.IsNull(srcConf.Get("testParameter3"));
		}

		[NUnit.Framework.Test]
		public virtual void Resolve()
		{
			Configuration conf = new Configuration(false);
			conf.Set("a", "A");
			conf.Set("b", "${a}");
			NUnit.Framework.Assert.AreEqual(conf.GetRaw("a"), "A");
			NUnit.Framework.Assert.AreEqual(conf.GetRaw("b"), "${a}");
			conf = ConfigurationUtils.Resolve(conf);
			NUnit.Framework.Assert.AreEqual(conf.GetRaw("a"), "A");
			NUnit.Framework.Assert.AreEqual(conf.GetRaw("b"), "A");
		}

		[NUnit.Framework.Test]
		public virtual void TestVarResolutionAndSysProps()
		{
			string userName = Runtime.GetProperty("user.name");
			Configuration conf = new Configuration(false);
			conf.Set("a", "A");
			conf.Set("b", "${a}");
			conf.Set("c", "${user.name}");
			conf.Set("d", "${aaa}");
			NUnit.Framework.Assert.AreEqual(conf.GetRaw("a"), "A");
			NUnit.Framework.Assert.AreEqual(conf.GetRaw("b"), "${a}");
			NUnit.Framework.Assert.AreEqual(conf.GetRaw("c"), "${user.name}");
			NUnit.Framework.Assert.AreEqual(conf.Get("a"), "A");
			NUnit.Framework.Assert.AreEqual(conf.Get("b"), "A");
			NUnit.Framework.Assert.AreEqual(conf.Get("c"), userName);
			NUnit.Framework.Assert.AreEqual(conf.Get("d"), "${aaa}");
			conf.Set("user.name", "foo");
			NUnit.Framework.Assert.AreEqual(conf.Get("user.name"), "foo");
		}
	}
}
