using System;
using System.Collections;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestJsonUtil
	{
		internal static FileStatus ToFileStatus(HdfsFileStatus f, string parent)
		{
			return new FileStatus(f.GetLen(), f.IsDir(), f.GetReplication(), f.GetBlockSize()
				, f.GetModificationTime(), f.GetAccessTime(), f.GetPermission(), f.GetOwner(), f
				.GetGroup(), f.IsSymlink() ? new Path(f.GetSymlink()) : null, new Path(f.GetFullName
				(parent)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHdfsFileStatus()
		{
			long now = Time.Now();
			string parent = "/dir";
			HdfsFileStatus status = new HdfsFileStatus(1001L, false, 3, 1L << 26, now, now + 
				10, new FsPermission((short)0x1a4), "user", "group", DFSUtil.String2Bytes("bar")
				, DFSUtil.String2Bytes("foo"), INodeId.GrandfatherInodeId, 0, null, unchecked((byte
				)0));
			FileStatus fstatus = ToFileStatus(status, parent);
			System.Console.Out.WriteLine("status  = " + status);
			System.Console.Out.WriteLine("fstatus = " + fstatus);
			string json = JsonUtil.ToJsonString(status, true);
			System.Console.Out.WriteLine("json    = " + json.Replace(",", ",\n  "));
			ObjectReader reader = new ObjectMapper().Reader(typeof(IDictionary));
			HdfsFileStatus s2 = JsonUtil.ToFileStatus((IDictionary<object, object>)reader.ReadValue
				(json), true);
			FileStatus fs2 = ToFileStatus(s2, parent);
			System.Console.Out.WriteLine("s2      = " + s2);
			System.Console.Out.WriteLine("fs2     = " + fs2);
			NUnit.Framework.Assert.AreEqual(fstatus, fs2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestToDatanodeInfoWithoutSecurePort()
		{
			IDictionary<string, object> response = new Dictionary<string, object>();
			response["ipAddr"] = "127.0.0.1";
			response["hostName"] = "localhost";
			response["storageID"] = "fake-id";
			response["xferPort"] = 1337l;
			response["infoPort"] = 1338l;
			// deliberately don't include an entry for "infoSecurePort"
			response["ipcPort"] = 1339l;
			response["capacity"] = 1024l;
			response["dfsUsed"] = 512l;
			response["remaining"] = 512l;
			response["blockPoolUsed"] = 512l;
			response["lastUpdate"] = 0l;
			response["xceiverCount"] = 4096l;
			response["networkLocation"] = "foo.bar.baz";
			response["adminState"] = "NORMAL";
			response["cacheCapacity"] = 123l;
			response["cacheUsed"] = 321l;
			JsonUtil.ToDatanodeInfo(response);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestToDatanodeInfoWithName()
		{
			IDictionary<string, object> response = new Dictionary<string, object>();
			// Older servers (1.x, 0.23, etc.) sends 'name' instead of ipAddr
			// and xferPort.
			string name = "127.0.0.1:1004";
			response["name"] = name;
			response["hostName"] = "localhost";
			response["storageID"] = "fake-id";
			response["infoPort"] = 1338l;
			response["ipcPort"] = 1339l;
			response["capacity"] = 1024l;
			response["dfsUsed"] = 512l;
			response["remaining"] = 512l;
			response["blockPoolUsed"] = 512l;
			response["lastUpdate"] = 0l;
			response["xceiverCount"] = 4096l;
			response["networkLocation"] = "foo.bar.baz";
			response["adminState"] = "NORMAL";
			response["cacheCapacity"] = 123l;
			response["cacheUsed"] = 321l;
			DatanodeInfo di = JsonUtil.ToDatanodeInfo(response);
			NUnit.Framework.Assert.AreEqual(name, di.GetXferAddr());
			// The encoded result should contain name, ipAddr and xferPort.
			IDictionary<string, object> r = JsonUtil.ToJsonMap(di);
			NUnit.Framework.Assert.AreEqual(name, r["name"]);
			NUnit.Framework.Assert.AreEqual("127.0.0.1", r["ipAddr"]);
			// In this test, it is Integer instead of Long since json was not actually
			// involved in constructing the map.
			NUnit.Framework.Assert.AreEqual(1004, (int)(int)r["xferPort"]);
			// Invalid names
			string[] badNames = new string[] { "127.0.0.1", "127.0.0.1:", ":", "127.0.0.1:sweet"
				, ":123" };
			foreach (string badName in badNames)
			{
				response["name"] = badName;
				CheckDecodeFailure(response);
			}
			// Missing both name and ipAddr
			Sharpen.Collections.Remove(response, "name");
			CheckDecodeFailure(response);
			// Only missing xferPort
			response["ipAddr"] = "127.0.0.1";
			CheckDecodeFailure(response);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestToAclStatus()
		{
			string jsonString = "{\"AclStatus\":{\"entries\":[\"user::rwx\",\"user:user1:rw-\",\"group::rw-\",\"other::r-x\"],\"group\":\"supergroup\",\"owner\":\"testuser\",\"stickyBit\":false}}";
			ObjectReader reader = new ObjectMapper().Reader(typeof(IDictionary));
			IDictionary<object, object> json = reader.ReadValue(jsonString);
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "user1", FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.ReadWrite), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.ReadExecute));
			AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
			aclStatusBuilder.Owner("testuser");
			aclStatusBuilder.Group("supergroup");
			aclStatusBuilder.AddEntries(aclSpec);
			aclStatusBuilder.StickyBit(false);
			NUnit.Framework.Assert.AreEqual("Should be equal", aclStatusBuilder.Build(), JsonUtil
				.ToAclStatus(json));
		}

		[NUnit.Framework.Test]
		public virtual void TestToJsonFromAclStatus()
		{
			string jsonString = "{\"AclStatus\":{\"entries\":[\"user:user1:rwx\",\"group::rw-\"],\"group\":\"supergroup\",\"owner\":\"testuser\",\"stickyBit\":false}}";
			AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
			aclStatusBuilder.Owner("testuser");
			aclStatusBuilder.Group("supergroup");
			aclStatusBuilder.StickyBit(false);
			IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.User, "user1", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.ReadWrite));
			aclStatusBuilder.AddEntries(aclSpec);
			NUnit.Framework.Assert.AreEqual(jsonString, JsonUtil.ToJsonString(aclStatusBuilder
				.Build()));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestToJsonFromXAttrs()
		{
			string jsonString = "{\"XAttrs\":[{\"name\":\"user.a1\",\"value\":\"0x313233\"},"
				 + "{\"name\":\"user.a2\",\"value\":\"0x313131\"}]}";
			XAttr xAttr1 = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.User).SetName("a1"
				).SetValue(XAttrCodec.DecodeValue("0x313233")).Build();
			XAttr xAttr2 = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.User).SetName("a2"
				).SetValue(XAttrCodec.DecodeValue("0x313131")).Build();
			IList<XAttr> xAttrs = Lists.NewArrayList();
			xAttrs.AddItem(xAttr1);
			xAttrs.AddItem(xAttr2);
			NUnit.Framework.Assert.AreEqual(jsonString, JsonUtil.ToJsonString(xAttrs, XAttrCodec
				.Hex));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestToXAttrMap()
		{
			string jsonString = "{\"XAttrs\":[{\"name\":\"user.a1\",\"value\":\"0x313233\"},"
				 + "{\"name\":\"user.a2\",\"value\":\"0x313131\"}]}";
			ObjectReader reader = new ObjectMapper().Reader(typeof(IDictionary));
			IDictionary<object, object> json = reader.ReadValue(jsonString);
			XAttr xAttr1 = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.User).SetName("a1"
				).SetValue(XAttrCodec.DecodeValue("0x313233")).Build();
			XAttr xAttr2 = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.User).SetName("a2"
				).SetValue(XAttrCodec.DecodeValue("0x313131")).Build();
			IList<XAttr> xAttrs = Lists.NewArrayList();
			xAttrs.AddItem(xAttr1);
			xAttrs.AddItem(xAttr2);
			IDictionary<string, byte[]> xAttrMap = XAttrHelper.BuildXAttrMap(xAttrs);
			IDictionary<string, byte[]> parsedXAttrMap = JsonUtil.ToXAttrs(json);
			NUnit.Framework.Assert.AreEqual(xAttrMap.Count, parsedXAttrMap.Count);
			IEnumerator<KeyValuePair<string, byte[]>> iter = xAttrMap.GetEnumerator();
			while (iter.HasNext())
			{
				KeyValuePair<string, byte[]> entry = iter.Next();
				Assert.AssertArrayEquals(entry.Value, parsedXAttrMap[entry.Key]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetXAttrFromJson()
		{
			string jsonString = "{\"XAttrs\":[{\"name\":\"user.a1\",\"value\":\"0x313233\"},"
				 + "{\"name\":\"user.a2\",\"value\":\"0x313131\"}]}";
			ObjectReader reader = new ObjectMapper().Reader(typeof(IDictionary));
			IDictionary<object, object> json = reader.ReadValue(jsonString);
			// Get xattr: user.a2
			byte[] value = JsonUtil.GetXAttr(json, "user.a2");
			Assert.AssertArrayEquals(XAttrCodec.DecodeValue("0x313131"), value);
		}

		private void CheckDecodeFailure(IDictionary<string, object> map)
		{
			try
			{
				JsonUtil.ToDatanodeInfo(map);
				NUnit.Framework.Assert.Fail("Exception not thrown against bad input.");
			}
			catch (Exception)
			{
			}
		}
		// expected
	}
}
