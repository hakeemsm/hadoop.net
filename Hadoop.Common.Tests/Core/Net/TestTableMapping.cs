using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	public class TestTableMapping
	{
		private string hostName1 = "1.2.3.4";

		private string hostName2 = "5.6.7.8";

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestResolve()
		{
			FilePath mapFile = FilePath.CreateTempFile(GetType().Name + ".testResolve", ".txt"
				);
			Files.Write(hostName1 + " /rack1\n" + hostName2 + "\t/rack2\n", mapFile, Charsets
				.Utf8);
			mapFile.DeleteOnExit();
			TableMapping mapping = new TableMapping();
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.NetTopologyTableMappingFileKey, mapFile.GetCanonicalPath
				());
			mapping.SetConf(conf);
			IList<string> names = new AList<string>();
			names.AddItem(hostName1);
			names.AddItem(hostName2);
			IList<string> result = mapping.Resolve(names);
			NUnit.Framework.Assert.AreEqual(names.Count, result.Count);
			NUnit.Framework.Assert.AreEqual("/rack1", result[0]);
			NUnit.Framework.Assert.AreEqual("/rack2", result[1]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTableCaching()
		{
			FilePath mapFile = FilePath.CreateTempFile(GetType().Name + ".testTableCaching", 
				".txt");
			Files.Write(hostName1 + " /rack1\n" + hostName2 + "\t/rack2\n", mapFile, Charsets
				.Utf8);
			mapFile.DeleteOnExit();
			TableMapping mapping = new TableMapping();
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.NetTopologyTableMappingFileKey, mapFile.GetCanonicalPath
				());
			mapping.SetConf(conf);
			IList<string> names = new AList<string>();
			names.AddItem(hostName1);
			names.AddItem(hostName2);
			IList<string> result1 = mapping.Resolve(names);
			NUnit.Framework.Assert.AreEqual(names.Count, result1.Count);
			NUnit.Framework.Assert.AreEqual("/rack1", result1[0]);
			NUnit.Framework.Assert.AreEqual("/rack2", result1[1]);
			// unset the file, see if it gets read again
			conf.Set(CommonConfigurationKeysPublic.NetTopologyTableMappingFileKey, "some bad value for a file"
				);
			IList<string> result2 = mapping.Resolve(names);
			NUnit.Framework.Assert.AreEqual(result1, result2);
		}

		[NUnit.Framework.Test]
		public virtual void TestNoFile()
		{
			TableMapping mapping = new TableMapping();
			Configuration conf = new Configuration();
			mapping.SetConf(conf);
			IList<string> names = new AList<string>();
			names.AddItem(hostName1);
			names.AddItem(hostName2);
			IList<string> result = mapping.Resolve(names);
			NUnit.Framework.Assert.AreEqual(names.Count, result.Count);
			NUnit.Framework.Assert.AreEqual(NetworkTopology.DefaultRack, result[0]);
			NUnit.Framework.Assert.AreEqual(NetworkTopology.DefaultRack, result[1]);
		}

		[NUnit.Framework.Test]
		public virtual void TestFileDoesNotExist()
		{
			TableMapping mapping = new TableMapping();
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.NetTopologyTableMappingFileKey, "/this/file/does/not/exist"
				);
			mapping.SetConf(conf);
			IList<string> names = new AList<string>();
			names.AddItem(hostName1);
			names.AddItem(hostName2);
			IList<string> result = mapping.Resolve(names);
			NUnit.Framework.Assert.AreEqual(names.Count, result.Count);
			NUnit.Framework.Assert.AreEqual(result[0], NetworkTopology.DefaultRack);
			NUnit.Framework.Assert.AreEqual(result[1], NetworkTopology.DefaultRack);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClearingCachedMappings()
		{
			FilePath mapFile = FilePath.CreateTempFile(GetType().Name + ".testClearingCachedMappings"
				, ".txt");
			Files.Write(hostName1 + " /rack1\n" + hostName2 + "\t/rack2\n", mapFile, Charsets
				.Utf8);
			mapFile.DeleteOnExit();
			TableMapping mapping = new TableMapping();
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.NetTopologyTableMappingFileKey, mapFile.GetCanonicalPath
				());
			mapping.SetConf(conf);
			IList<string> names = new AList<string>();
			names.AddItem(hostName1);
			names.AddItem(hostName2);
			IList<string> result = mapping.Resolve(names);
			NUnit.Framework.Assert.AreEqual(names.Count, result.Count);
			NUnit.Framework.Assert.AreEqual("/rack1", result[0]);
			NUnit.Framework.Assert.AreEqual("/rack2", result[1]);
			Files.Write(string.Empty, mapFile, Charsets.Utf8);
			mapping.ReloadCachedMappings();
			names = new AList<string>();
			names.AddItem(hostName1);
			names.AddItem(hostName2);
			result = mapping.Resolve(names);
			NUnit.Framework.Assert.AreEqual(names.Count, result.Count);
			NUnit.Framework.Assert.AreEqual(NetworkTopology.DefaultRack, result[0]);
			NUnit.Framework.Assert.AreEqual(NetworkTopology.DefaultRack, result[1]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBadFile()
		{
			FilePath mapFile = FilePath.CreateTempFile(GetType().Name + ".testBadFile", ".txt"
				);
			Files.Write("bad contents", mapFile, Charsets.Utf8);
			mapFile.DeleteOnExit();
			TableMapping mapping = new TableMapping();
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.NetTopologyTableMappingFileKey, mapFile.GetCanonicalPath
				());
			mapping.SetConf(conf);
			IList<string> names = new AList<string>();
			names.AddItem(hostName1);
			names.AddItem(hostName2);
			IList<string> result = mapping.Resolve(names);
			NUnit.Framework.Assert.AreEqual(names.Count, result.Count);
			NUnit.Framework.Assert.AreEqual(result[0], NetworkTopology.DefaultRack);
			NUnit.Framework.Assert.AreEqual(result[1], NetworkTopology.DefaultRack);
		}
	}
}
