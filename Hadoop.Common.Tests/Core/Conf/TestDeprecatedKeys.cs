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
			Assert.True(scriptFile.Equals("xyz"));
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
			Assert.True(fileContents.Contains("old.config.yet.to.be.deprecated"
				));
			Assert.True(fileContents.Contains("new.conf.to.replace.deprecated.conf"
				));
		}

		[Fact]
		public virtual void TestIteratorWithDeprecatedKeysMappedToMultipleNewKeys()
		{
			Configuration conf = new Configuration();
			Configuration.AddDeprecation("dK", new string[] { "nK1", "nK2" });
			conf.Set("k", "v");
			conf.Set("dK", "V");
			Assert.Equal("V", conf.Get("dK"));
			Assert.Equal("V", conf.Get("nK1"));
			Assert.Equal("V", conf.Get("nK2"));
			conf.Set("nK1", "VV");
			Assert.Equal("VV", conf.Get("dK"));
			Assert.Equal("VV", conf.Get("nK1"));
			Assert.Equal("VV", conf.Get("nK2"));
			conf.Set("nK2", "VVV");
			Assert.Equal("VVV", conf.Get("dK"));
			Assert.Equal("VVV", conf.Get("nK2"));
			Assert.Equal("VVV", conf.Get("nK1"));
			bool kFound = false;
			bool dKFound = false;
			bool nK1Found = false;
			bool nK2Found = false;
			foreach (KeyValuePair<string, string> entry in conf)
			{
				if (entry.Key.Equals("k"))
				{
					Assert.Equal("v", entry.Value);
					kFound = true;
				}
				if (entry.Key.Equals("dK"))
				{
					Assert.Equal("VVV", entry.Value);
					dKFound = true;
				}
				if (entry.Key.Equals("nK1"))
				{
					Assert.Equal("VVV", entry.Value);
					nK1Found = true;
				}
				if (entry.Key.Equals("nK2"))
				{
					Assert.Equal("VVV", entry.Value);
					nK2Found = true;
				}
			}
			Assert.True("regular Key not found", kFound);
			Assert.True("deprecated Key not found", dKFound);
			Assert.True("new Key 1 not found", nK1Found);
			Assert.True("new Key 2 not found", nK2Found);
		}
	}
}
