using Sharpen;

namespace org.apache.hadoop.conf
{
	public class TestDeprecatedKeys : NUnit.Framework.TestCase
	{
		//Tests a deprecated key
		/// <exception cref="System.Exception"/>
		public virtual void testDeprecatedKeys()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("topology.script.file.name", "xyz");
			conf.set("topology.script.file.name", "xyz");
			string scriptFile = conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY
				);
			NUnit.Framework.Assert.IsTrue(scriptFile.Equals("xyz"));
		}

		//Tests reading / writing a conf file with deprecation after setting
		/// <exception cref="System.Exception"/>
		public virtual void testReadWriteWithDeprecatedKeys()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean("old.config.yet.to.be.deprecated", true);
			org.apache.hadoop.conf.Configuration.addDeprecation("old.config.yet.to.be.deprecated"
				, new string[] { "new.conf.to.replace.deprecated.conf" });
			java.io.ByteArrayOutputStream @out = new java.io.ByteArrayOutputStream();
			string fileContents;
			try
			{
				conf.writeXml(@out);
				fileContents = @out.ToString();
			}
			finally
			{
				@out.close();
			}
			NUnit.Framework.Assert.IsTrue(fileContents.contains("old.config.yet.to.be.deprecated"
				));
			NUnit.Framework.Assert.IsTrue(fileContents.contains("new.conf.to.replace.deprecated.conf"
				));
		}

		[NUnit.Framework.Test]
		public virtual void testIteratorWithDeprecatedKeysMappedToMultipleNewKeys()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.conf.Configuration.addDeprecation("dK", new string[] { "nK1", "nK2"
				 });
			conf.set("k", "v");
			conf.set("dK", "V");
			NUnit.Framework.Assert.AreEqual("V", conf.get("dK"));
			NUnit.Framework.Assert.AreEqual("V", conf.get("nK1"));
			NUnit.Framework.Assert.AreEqual("V", conf.get("nK2"));
			conf.set("nK1", "VV");
			NUnit.Framework.Assert.AreEqual("VV", conf.get("dK"));
			NUnit.Framework.Assert.AreEqual("VV", conf.get("nK1"));
			NUnit.Framework.Assert.AreEqual("VV", conf.get("nK2"));
			conf.set("nK2", "VVV");
			NUnit.Framework.Assert.AreEqual("VVV", conf.get("dK"));
			NUnit.Framework.Assert.AreEqual("VVV", conf.get("nK2"));
			NUnit.Framework.Assert.AreEqual("VVV", conf.get("nK1"));
			bool kFound = false;
			bool dKFound = false;
			bool nK1Found = false;
			bool nK2Found = false;
			foreach (System.Collections.Generic.KeyValuePair<string, string> entry in conf)
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
