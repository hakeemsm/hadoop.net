using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	public class TestDeprecatedKeys : TestCase
	{
		//Tests a deprecated key
		/// <exception cref="System.Exception"/>
		public virtual void TestDeprecatedKeys()
		{
			Configuration conf = new Configuration();
			conf.Set("topology.script.file.name", "xyz");
			conf.Set("topology.script.file.name", "xyz");
			string scriptFile = conf.Get(CommonConfigurationKeys.NetTopologyScriptFileNameKey
				);
			NUnit.Framework.Assert.IsTrue(scriptFile.Equals("xyz"));
		}

		//Tests reading / writing a conf file with deprecation after setting
		/// <exception cref="System.Exception"/>
		public virtual void TestReadWriteWithDeprecatedKeys()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean("old.config.yet.to.be.deprecated", true);
			Configuration.AddDeprecation("old.config.yet.to.be.deprecated", new string[] { "new.conf.to.replace.deprecated.conf"
				 });
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			string fileContents;
			try
			{
				conf.WriteXml(@out);
				fileContents = @out.ToString();
			}
			finally
			{
				@out.Close();
			}
			NUnit.Framework.Assert.IsTrue(fileContents.Contains("old.config.yet.to.be.deprecated"
				));
			NUnit.Framework.Assert.IsTrue(fileContents.Contains("new.conf.to.replace.deprecated.conf"
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestIteratorWithDeprecatedKeysMappedToMultipleNewKeys()
		{
			Configuration conf = new Configuration();
			Configuration.AddDeprecation("dK", new string[] { "nK1", "nK2" });
			conf.Set("k", "v");
			conf.Set("dK", "V");
			NUnit.Framework.Assert.AreEqual("V", conf.Get("dK"));
			NUnit.Framework.Assert.AreEqual("V", conf.Get("nK1"));
			NUnit.Framework.Assert.AreEqual("V", conf.Get("nK2"));
			conf.Set("nK1", "VV");
			NUnit.Framework.Assert.AreEqual("VV", conf.Get("dK"));
			NUnit.Framework.Assert.AreEqual("VV", conf.Get("nK1"));
			NUnit.Framework.Assert.AreEqual("VV", conf.Get("nK2"));
			conf.Set("nK2", "VVV");
			NUnit.Framework.Assert.AreEqual("VVV", conf.Get("dK"));
			NUnit.Framework.Assert.AreEqual("VVV", conf.Get("nK2"));
			NUnit.Framework.Assert.AreEqual("VVV", conf.Get("nK1"));
			bool kFound = false;
			bool dKFound = false;
			bool nK1Found = false;
			bool nK2Found = false;
			foreach (KeyValuePair<string, string> entry in conf)
			{
				if (entry.Key.Equals("k"))
				{
					NUnit.Framework.Assert.AreEqual("v", entry.Value);
					kFound = true;
				}
				if (entry.Key.Equals("dK"))
				{
					NUnit.Framework.Assert.AreEqual("VVV", entry.Value);
					dKFound = true;
				}
				if (entry.Key.Equals("nK1"))
				{
					NUnit.Framework.Assert.AreEqual("VVV", entry.Value);
					nK1Found = true;
				}
				if (entry.Key.Equals("nK2"))
				{
					NUnit.Framework.Assert.AreEqual("VVV", entry.Value);
					nK2Found = true;
				}
			}
			NUnit.Framework.Assert.IsTrue("regular Key not found", kFound);
			NUnit.Framework.Assert.IsTrue("deprecated Key not found", dKFound);
			NUnit.Framework.Assert.IsTrue("new Key 1 not found", nK1Found);
			NUnit.Framework.Assert.IsTrue("new Key 2 not found", nK2Found);
		}
	}
}
