using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestShellBasedIdMapping
	{
		private static readonly IDictionary<int, int> EmptyPassThroughMap = new ShellBasedIdMapping.PassThroughMap
			<int>();

		/// <exception cref="System.IO.IOException"/>
		private void CreateStaticMapFile(FilePath smapFile, string smapStr)
		{
			OutputStream @out = new FileOutputStream(smapFile);
			@out.Write(Sharpen.Runtime.GetBytesForString(smapStr));
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStaticMapParsing()
		{
			FilePath tempStaticMapFile = FilePath.CreateTempFile("nfs-", ".map");
			string staticMapFileContents = "uid 10 100\n" + "gid 10 200\n" + "uid 11 201 # comment at the end of a line\n"
				 + "uid 12 301\n" + "# Comment at the beginning of a line\n" + "    # Comment that starts late in the line\n"
				 + "uid 10000 10001# line without whitespace before comment\n" + "uid 13 302\n" 
				+ "gid\t11\t201\n" + "\n" + "gid 12 202\n" + "uid 4294967294 123\n" + "gid 4294967295 321";
			// Tabs instead of spaces.
			// Entirely empty line.
			CreateStaticMapFile(tempStaticMapFile, staticMapFileContents);
			ShellBasedIdMapping.StaticMapping parsedMap = ShellBasedIdMapping.ParseStaticMap(
				tempStaticMapFile);
			NUnit.Framework.Assert.AreEqual(10, (int)parsedMap.uidMapping[100]);
			NUnit.Framework.Assert.AreEqual(11, (int)parsedMap.uidMapping[201]);
			NUnit.Framework.Assert.AreEqual(12, (int)parsedMap.uidMapping[301]);
			NUnit.Framework.Assert.AreEqual(13, (int)parsedMap.uidMapping[302]);
			NUnit.Framework.Assert.AreEqual(10, (int)parsedMap.gidMapping[200]);
			NUnit.Framework.Assert.AreEqual(11, (int)parsedMap.gidMapping[201]);
			NUnit.Framework.Assert.AreEqual(12, (int)parsedMap.gidMapping[202]);
			NUnit.Framework.Assert.AreEqual(10000, (int)parsedMap.uidMapping[10001]);
			// Ensure pass-through of unmapped IDs works.
			NUnit.Framework.Assert.AreEqual(1000, (int)parsedMap.uidMapping[1000]);
			NUnit.Framework.Assert.AreEqual(-2, (int)parsedMap.uidMapping[123]);
			NUnit.Framework.Assert.AreEqual(-1, (int)parsedMap.gidMapping[321]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStaticMapping()
		{
			Assume.AssumeTrue(!Shell.Windows);
			IDictionary<int, int> uidStaticMap = new ShellBasedIdMapping.PassThroughMap<int>(
				);
			IDictionary<int, int> gidStaticMap = new ShellBasedIdMapping.PassThroughMap<int>(
				);
			uidStaticMap[11501] = 10;
			gidStaticMap[497] = 200;
			// Maps for id to name map
			BiMap<int, string> uMap = HashBiMap.Create();
			BiMap<int, string> gMap = HashBiMap.Create();
			string GetAllUsersCmd = "echo \"atm:x:1000:1000:Aaron T. Myers,,,:/home/atm:/bin/bash\n"
				 + "hdfs:x:11501:10787:Grid Distributed File System:/home/hdfs:/bin/bash\"" + " | cut -d: -f1,3";
			string GetAllGroupsCmd = "echo \"hdfs:*:11501:hrt_hdfs\n" + "mapred:x:497\n" + "mapred2:x:498\""
				 + " | cut -d: -f1,3";
			ShellBasedIdMapping.UpdateMapInternal(uMap, "user", GetAllUsersCmd, ":", uidStaticMap
				);
			ShellBasedIdMapping.UpdateMapInternal(gMap, "group", GetAllGroupsCmd, ":", gidStaticMap
				);
			NUnit.Framework.Assert.AreEqual("hdfs", uMap[10]);
			NUnit.Framework.Assert.AreEqual(10, (int)uMap.Inverse()["hdfs"]);
			NUnit.Framework.Assert.AreEqual("atm", uMap[1000]);
			NUnit.Framework.Assert.AreEqual(1000, (int)uMap.Inverse()["atm"]);
			NUnit.Framework.Assert.AreEqual("hdfs", gMap[11501]);
			NUnit.Framework.Assert.AreEqual(11501, (int)gMap.Inverse()["hdfs"]);
			NUnit.Framework.Assert.AreEqual("mapred", gMap[200]);
			NUnit.Framework.Assert.AreEqual(200, (int)gMap.Inverse()["mapred"]);
			NUnit.Framework.Assert.AreEqual("mapred2", gMap[498]);
			NUnit.Framework.Assert.AreEqual(498, (int)gMap.Inverse()["mapred2"]);
		}

		// Test staticMap refreshing
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStaticMapUpdate()
		{
			Assume.AssumeTrue(!Shell.Windows);
			FilePath tempStaticMapFile = FilePath.CreateTempFile("nfs-", ".map");
			tempStaticMapFile.Delete();
			Configuration conf = new Configuration();
			conf.SetLong(IdMappingConstant.UsergroupidUpdateMillisKey, 1000);
			conf.Set(IdMappingConstant.StaticIdMappingFileKey, tempStaticMapFile.GetPath());
			ShellBasedIdMapping refIdMapping = new ShellBasedIdMapping(conf, true);
			ShellBasedIdMapping incrIdMapping = new ShellBasedIdMapping(conf);
			BiMap<int, string> uidNameMap = refIdMapping.GetUidNameMap();
			BiMap<int, string> gidNameMap = refIdMapping.GetGidNameMap();
			// Force empty map, to see effect of incremental map update of calling
			// getUid()
			incrIdMapping.ClearNameMaps();
			uidNameMap = refIdMapping.GetUidNameMap();
			{
				KeyValuePair<int, string> me = uidNameMap.GetEnumerator().Next();
				int id = me.Key;
				string name = me.Value;
				// The static map is empty, so the id found for "name" would be
				// the same as "id"
				int nid = incrIdMapping.GetUid(name);
				NUnit.Framework.Assert.AreEqual(id, nid);
				// Clear map and update staticMap file
				incrIdMapping.ClearNameMaps();
				int rid = id + 10000;
				string smapStr = "uid " + rid + " " + id;
				CreateStaticMapFile(tempStaticMapFile, smapStr);
				// Now the id found for "name" should be the id specified by
				// the staticMap
				nid = incrIdMapping.GetUid(name);
				NUnit.Framework.Assert.AreEqual(rid, nid);
			}
			// Force empty map, to see effect of incremental map update of calling
			// getGid()
			incrIdMapping.ClearNameMaps();
			gidNameMap = refIdMapping.GetGidNameMap();
			{
				KeyValuePair<int, string> me = gidNameMap.GetEnumerator().Next();
				int id = me.Key;
				string name = me.Value;
				// The static map is empty, so the id found for "name" would be
				// the same as "id"
				int nid = incrIdMapping.GetGid(name);
				NUnit.Framework.Assert.AreEqual(id, nid);
				// Clear map and update staticMap file
				incrIdMapping.ClearNameMaps();
				int rid = id + 10000;
				string smapStr = "gid " + rid + " " + id;
				// Sleep a bit to avoid that two changes have the same modification time
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				CreateStaticMapFile(tempStaticMapFile, smapStr);
				// Now the id found for "name" should be the id specified by
				// the staticMap
				nid = incrIdMapping.GetGid(name);
				NUnit.Framework.Assert.AreEqual(rid, nid);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDuplicates()
		{
			Assume.AssumeTrue(!Shell.Windows);
			string GetAllUsersCmd = "echo \"root:x:0:0:root:/root:/bin/bash\n" + "hdfs:x:11501:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n"
				 + "hdfs:x:11502:10788:Grid Distributed File System:/home/hdfs:/bin/bash\n" + "hdfs1:x:11501:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n"
				 + "hdfs2:x:11502:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n" + "bin:x:2:2:bin:/bin:/bin/sh\n"
				 + "bin:x:1:1:bin:/bin:/sbin/nologin\n" + "daemon:x:1:1:daemon:/usr/sbin:/bin/sh\n"
				 + "daemon:x:2:2:daemon:/sbin:/sbin/nologin\"" + " | cut -d: -f1,3";
			string GetAllGroupsCmd = "echo \"hdfs:*:11501:hrt_hdfs\n" + "mapred:x:497\n" + "mapred2:x:497\n"
				 + "mapred:x:498\n" + "mapred3:x:498\"" + " | cut -d: -f1,3";
			// Maps for id to name map
			BiMap<int, string> uMap = HashBiMap.Create();
			BiMap<int, string> gMap = HashBiMap.Create();
			ShellBasedIdMapping.UpdateMapInternal(uMap, "user", GetAllUsersCmd, ":", EmptyPassThroughMap
				);
			NUnit.Framework.Assert.AreEqual(5, uMap.Count);
			NUnit.Framework.Assert.AreEqual("root", uMap[0]);
			NUnit.Framework.Assert.AreEqual("hdfs", uMap[11501]);
			NUnit.Framework.Assert.AreEqual("hdfs2", uMap[11502]);
			NUnit.Framework.Assert.AreEqual("bin", uMap[2]);
			NUnit.Framework.Assert.AreEqual("daemon", uMap[1]);
			ShellBasedIdMapping.UpdateMapInternal(gMap, "group", GetAllGroupsCmd, ":", EmptyPassThroughMap
				);
			NUnit.Framework.Assert.IsTrue(gMap.Count == 3);
			NUnit.Framework.Assert.AreEqual("hdfs", gMap[11501]);
			NUnit.Framework.Assert.AreEqual("mapred", gMap[497]);
			NUnit.Framework.Assert.AreEqual("mapred3", gMap[498]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestIdOutOfIntegerRange()
		{
			Assume.AssumeTrue(!Shell.Windows);
			string GetAllUsersCmd = "echo \"" + "nfsnobody:x:4294967294:4294967294:Anonymous NFS User:/var/lib/nfs:/sbin/nologin\n"
				 + "nfsnobody1:x:4294967295:4294967295:Anonymous NFS User:/var/lib/nfs1:/sbin/nologin\n"
				 + "maxint:x:2147483647:2147483647:Grid Distributed File System:/home/maxint:/bin/bash\n"
				 + "minint:x:2147483648:2147483648:Grid Distributed File System:/home/minint:/bin/bash\n"
				 + "archivebackup:*:1031:4294967294:Archive Backup:/home/users/archivebackup:/bin/sh\n"
				 + "hdfs:x:11501:10787:Grid Distributed File System:/home/hdfs:/bin/bash\n" + "daemon:x:2:2:daemon:/sbin:/sbin/nologin\""
				 + " | cut -d: -f1,3";
			string GetAllGroupsCmd = "echo \"" + "hdfs:*:11501:hrt_hdfs\n" + "rpcuser:*:29:\n"
				 + "nfsnobody:*:4294967294:\n" + "nfsnobody1:*:4294967295:\n" + "maxint:*:2147483647:\n"
				 + "minint:*:2147483648:\n" + "mapred3:x:498\"" + " | cut -d: -f1,3";
			// Maps for id to name map
			BiMap<int, string> uMap = HashBiMap.Create();
			BiMap<int, string> gMap = HashBiMap.Create();
			ShellBasedIdMapping.UpdateMapInternal(uMap, "user", GetAllUsersCmd, ":", EmptyPassThroughMap
				);
			NUnit.Framework.Assert.IsTrue(uMap.Count == 7);
			NUnit.Framework.Assert.AreEqual("nfsnobody", uMap[-2]);
			NUnit.Framework.Assert.AreEqual("nfsnobody1", uMap[-1]);
			NUnit.Framework.Assert.AreEqual("maxint", uMap[2147483647]);
			NUnit.Framework.Assert.AreEqual("minint", uMap[-2147483648]);
			NUnit.Framework.Assert.AreEqual("archivebackup", uMap[1031]);
			NUnit.Framework.Assert.AreEqual("hdfs", uMap[11501]);
			NUnit.Framework.Assert.AreEqual("daemon", uMap[2]);
			ShellBasedIdMapping.UpdateMapInternal(gMap, "group", GetAllGroupsCmd, ":", EmptyPassThroughMap
				);
			NUnit.Framework.Assert.IsTrue(gMap.Count == 7);
			NUnit.Framework.Assert.AreEqual("hdfs", gMap[11501]);
			NUnit.Framework.Assert.AreEqual("rpcuser", gMap[29]);
			NUnit.Framework.Assert.AreEqual("nfsnobody", gMap[-2]);
			NUnit.Framework.Assert.AreEqual("nfsnobody1", gMap[-1]);
			NUnit.Framework.Assert.AreEqual("maxint", gMap[2147483647]);
			NUnit.Framework.Assert.AreEqual("minint", gMap[-2147483648]);
			NUnit.Framework.Assert.AreEqual("mapred3", gMap[498]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUserUpdateSetting()
		{
			ShellBasedIdMapping iug = new ShellBasedIdMapping(new Configuration());
			NUnit.Framework.Assert.AreEqual(iug.GetTimeout(), IdMappingConstant.UsergroupidUpdateMillisDefault
				);
			Configuration conf = new Configuration();
			conf.SetLong(IdMappingConstant.UsergroupidUpdateMillisKey, 0);
			iug = new ShellBasedIdMapping(conf);
			NUnit.Framework.Assert.AreEqual(iug.GetTimeout(), IdMappingConstant.UsergroupidUpdateMillisMin
				);
			conf.SetLong(IdMappingConstant.UsergroupidUpdateMillisKey, IdMappingConstant.UsergroupidUpdateMillisDefault
				 * 2);
			iug = new ShellBasedIdMapping(conf);
			NUnit.Framework.Assert.AreEqual(iug.GetTimeout(), IdMappingConstant.UsergroupidUpdateMillisDefault
				 * 2);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateMapIncr()
		{
			Configuration conf = new Configuration();
			conf.SetLong(IdMappingConstant.UsergroupidUpdateMillisKey, 600000);
			ShellBasedIdMapping refIdMapping = new ShellBasedIdMapping(conf, true);
			ShellBasedIdMapping incrIdMapping = new ShellBasedIdMapping(conf);
			// Command such as "getent passwd <userName>" will return empty string if
			// <username> is numerical, remove them from the map for testing purpose.
			BiMap<int, string> uidNameMap = refIdMapping.GetUidNameMap();
			BiMap<int, string> gidNameMap = refIdMapping.GetGidNameMap();
			// Force empty map, to see effect of incremental map update of calling
			// getUserName()
			incrIdMapping.ClearNameMaps();
			uidNameMap = refIdMapping.GetUidNameMap();
			foreach (KeyValuePair<int, string> me in uidNameMap)
			{
				int id = me.Key;
				string name = me.Value;
				string tname = incrIdMapping.GetUserName(id, null);
				NUnit.Framework.Assert.AreEqual(name, tname);
			}
			NUnit.Framework.Assert.AreEqual(uidNameMap.Count, incrIdMapping.GetUidNameMap().Count
				);
			// Force empty map, to see effect of incremental map update of calling
			// getUid()
			incrIdMapping.ClearNameMaps();
			foreach (KeyValuePair<int, string> me_1 in uidNameMap)
			{
				int id = me_1.Key;
				string name = me_1.Value;
				int tid = incrIdMapping.GetUid(name);
				NUnit.Framework.Assert.AreEqual(id, tid);
			}
			NUnit.Framework.Assert.AreEqual(uidNameMap.Count, incrIdMapping.GetUidNameMap().Count
				);
			// Force empty map, to see effect of incremental map update of calling
			// getGroupName()
			incrIdMapping.ClearNameMaps();
			gidNameMap = refIdMapping.GetGidNameMap();
			foreach (KeyValuePair<int, string> me_2 in gidNameMap)
			{
				int id = me_2.Key;
				string name = me_2.Value;
				string tname = incrIdMapping.GetGroupName(id, null);
				NUnit.Framework.Assert.AreEqual(name, tname);
			}
			NUnit.Framework.Assert.AreEqual(gidNameMap.Count, incrIdMapping.GetGidNameMap().Count
				);
			// Force empty map, to see effect of incremental map update of calling
			// getGid()
			incrIdMapping.ClearNameMaps();
			gidNameMap = refIdMapping.GetGidNameMap();
			foreach (KeyValuePair<int, string> me_3 in gidNameMap)
			{
				int id = me_3.Key;
				string name = me_3.Value;
				int tid = incrIdMapping.GetGid(name);
				NUnit.Framework.Assert.AreEqual(id, tid);
			}
			NUnit.Framework.Assert.AreEqual(gidNameMap.Count, incrIdMapping.GetGidNameMap().Count
				);
		}
	}
}
