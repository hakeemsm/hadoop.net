using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Test
	/// <see cref="FSDirectory"/>
	/// , the in-memory namespace tree.
	/// </summary>
	public class TestFSDirectory
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestFSDirectory));

		private const long seed = 0;

		private const short Replication = 3;

		private readonly Path dir = new Path("/" + GetType().Name);

		private readonly Path sub1;

		private readonly Path file1;

		private readonly Path file2;

		private readonly Path sub11;

		private readonly Path file3;

		private readonly Path file5;

		private readonly Path sub2;

		private readonly Path file6;

		private Configuration conf;

		private MiniDFSCluster cluster;

		private FSNamesystem fsn;

		private FSDirectory fsdir;

		private DistributedFileSystem hdfs;

		private const int numGeneratedXAttrs = 256;

		private static readonly ImmutableList<XAttr> generatedXAttrs = ImmutableList.CopyOf
			(GenerateXAttrs(numGeneratedXAttrs));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxXattrsPerInodeKey, 2);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
			hdfs = cluster.GetFileSystem();
			DFSTestUtil.CreateFile(hdfs, file1, 1024, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file2, 1024, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file3, 1024, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file5, 1024, Replication, seed);
			hdfs.Mkdirs(sub2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Dump the tree, make some changes, and then dump the tree again.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDumpTree()
		{
			INode root = fsdir.GetINode("/");
			Log.Info("Original tree");
			StringBuilder b1 = root.DumpTreeRecursively();
			System.Console.Out.WriteLine("b1=" + b1);
			BufferedReader @in = new BufferedReader(new StringReader(b1.ToString()));
			string line = @in.ReadLine();
			CheckClassName(line);
			for (; (line = @in.ReadLine()) != null; )
			{
				line = line.Trim();
				if (!line.IsEmpty() && !line.Contains("snapshot"))
				{
					NUnit.Framework.Assert.IsTrue("line=" + line, line.StartsWith(INodeDirectory.DumptreeLastItem
						) || line.StartsWith(INodeDirectory.DumptreeExceptLastItem));
					CheckClassName(line);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSkipQuotaCheck()
		{
			try
			{
				// set quota. nsQuota of 1 means no files can be created
				//  under this directory.
				hdfs.SetQuota(sub2, 1, long.MaxValue);
				// create a file
				try
				{
					// this should fail
					DFSTestUtil.CreateFile(hdfs, file6, 1024, Replication, seed);
					throw new IOException("The create should have failed.");
				}
				catch (NSQuotaExceededException)
				{
				}
				// ignored
				// disable the quota check and retry. this should succeed.
				fsdir.DisableQuotaChecks();
				DFSTestUtil.CreateFile(hdfs, file6, 1024, Replication, seed);
				// trying again after re-enabling the check.
				hdfs.Delete(file6, false);
				// cleanup
				fsdir.EnableQuotaChecks();
				try
				{
					// this should fail
					DFSTestUtil.CreateFile(hdfs, file6, 1024, Replication, seed);
					throw new IOException("The create should have failed.");
				}
				catch (NSQuotaExceededException)
				{
				}
			}
			finally
			{
				// ignored
				hdfs.Delete(file6, false);
				// cleanup, in case the test failed in the middle.
				hdfs.SetQuota(sub2, long.MaxValue, long.MaxValue);
			}
		}

		internal static void CheckClassName(string line)
		{
			int i = line.LastIndexOf('(');
			int j = line.LastIndexOf('@');
			string classname = Sharpen.Runtime.Substring(line, i + 1, j);
			NUnit.Framework.Assert.IsTrue(classname.StartsWith(typeof(INodeFile).Name) || classname
				.StartsWith(typeof(INodeDirectory).Name));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestINodeXAttrsLimit()
		{
			IList<XAttr> existingXAttrs = Lists.NewArrayListWithCapacity(2);
			XAttr xAttr1 = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.User).SetName("a1"
				).SetValue(new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
				(int)(0x33)) }).Build();
			XAttr xAttr2 = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.User).SetName("a2"
				).SetValue(new byte[] { unchecked((int)(0x31)), unchecked((int)(0x31)), unchecked(
				(int)(0x31)) }).Build();
			existingXAttrs.AddItem(xAttr1);
			existingXAttrs.AddItem(xAttr2);
			// Adding system and raw namespace xAttrs aren't affected by inode
			// xAttrs limit.
			XAttr newSystemXAttr = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.System)
				.SetName("a3").SetValue(new byte[] { unchecked((int)(0x33)), unchecked((int)(0x33
				)), unchecked((int)(0x33)) }).Build();
			XAttr newRawXAttr = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.Raw).SetName
				("a3").SetValue(new byte[] { unchecked((int)(0x33)), unchecked((int)(0x33)), unchecked(
				(int)(0x33)) }).Build();
			IList<XAttr> newXAttrs = Lists.NewArrayListWithCapacity(2);
			newXAttrs.AddItem(newSystemXAttr);
			newXAttrs.AddItem(newRawXAttr);
			IList<XAttr> xAttrs = FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, newXAttrs
				, EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace));
			NUnit.Framework.Assert.AreEqual(xAttrs.Count, 4);
			// Adding a trusted namespace xAttr, is affected by inode xAttrs limit.
			XAttr newXAttr1 = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.Trusted).SetName
				("a4").SetValue(new byte[] { unchecked((int)(0x34)), unchecked((int)(0x34)), unchecked(
				(int)(0x34)) }).Build();
			newXAttrs.Set(0, newXAttr1);
			try
			{
				FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, newXAttrs, EnumSet.Of(XAttrSetFlag
					.Create, XAttrSetFlag.Replace));
				NUnit.Framework.Assert.Fail("Setting user visible xattr on inode should fail if "
					 + "reaching limit.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot add additional XAttr " + "to inode, would exceed limit"
					, e);
			}
		}

		/// <summary>
		/// Verify that the first <i>num</i> generatedXAttrs are present in
		/// newXAttrs.
		/// </summary>
		private static void VerifyXAttrsPresent(IList<XAttr> newXAttrs, int num)
		{
			NUnit.Framework.Assert.AreEqual("Unexpected number of XAttrs after multiset", num
				, newXAttrs.Count);
			for (int i = 0; i < num; i++)
			{
				XAttr search = generatedXAttrs[i];
				NUnit.Framework.Assert.IsTrue("Did not find set XAttr " + search + " + after multiset"
					, newXAttrs.Contains(search));
			}
		}

		private static IList<XAttr> GenerateXAttrs(int numXAttrs)
		{
			IList<XAttr> generatedXAttrs = Lists.NewArrayListWithCapacity(numXAttrs);
			for (int i = 0; i < numXAttrs; i++)
			{
				XAttr xAttr = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.System).SetName(
					"a" + i).SetValue(new byte[] { unchecked((byte)i), unchecked((byte)(i + 1)), unchecked(
					(byte)(i + 2)) }).Build();
				generatedXAttrs.AddItem(xAttr);
			}
			return generatedXAttrs;
		}

		/// <summary>Test setting and removing multiple xattrs via single operations</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestXAttrMultiSetRemove()
		{
			IList<XAttr> existingXAttrs = Lists.NewArrayListWithCapacity(0);
			// Keep adding a random number of xattrs and verifying until exhausted
			Random rand = new Random(unchecked((int)(0xFEEDA)));
			int numExpectedXAttrs = 0;
			while (numExpectedXAttrs < numGeneratedXAttrs)
			{
				Log.Info("Currently have " + numExpectedXAttrs + " xattrs");
				int numToAdd = rand.Next(5) + 1;
				IList<XAttr> toAdd = Lists.NewArrayListWithCapacity(numToAdd);
				for (int i = 0; i < numToAdd; i++)
				{
					if (numExpectedXAttrs >= numGeneratedXAttrs)
					{
						break;
					}
					toAdd.AddItem(generatedXAttrs[numExpectedXAttrs]);
					numExpectedXAttrs++;
				}
				Log.Info("Attempting to add " + toAdd.Count + " XAttrs");
				for (int i_1 = 0; i_1 < toAdd.Count; i_1++)
				{
					Log.Info("Will add XAttr " + toAdd[i_1]);
				}
				IList<XAttr> newXAttrs = FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, toAdd
					, EnumSet.Of(XAttrSetFlag.Create));
				VerifyXAttrsPresent(newXAttrs, numExpectedXAttrs);
				existingXAttrs = newXAttrs;
			}
			// Keep removing a random number of xattrs and verifying until all gone
			while (numExpectedXAttrs > 0)
			{
				Log.Info("Currently have " + numExpectedXAttrs + " xattrs");
				int numToRemove = rand.Next(5) + 1;
				IList<XAttr> toRemove = Lists.NewArrayListWithCapacity(numToRemove);
				for (int i = 0; i < numToRemove; i++)
				{
					if (numExpectedXAttrs == 0)
					{
						break;
					}
					toRemove.AddItem(generatedXAttrs[numExpectedXAttrs - 1]);
					numExpectedXAttrs--;
				}
				int expectedNumToRemove = toRemove.Count;
				Log.Info("Attempting to remove " + expectedNumToRemove + " XAttrs");
				IList<XAttr> removedXAttrs = Lists.NewArrayList();
				IList<XAttr> newXAttrs = FSDirXAttrOp.FilterINodeXAttrs(existingXAttrs, toRemove, 
					removedXAttrs);
				NUnit.Framework.Assert.AreEqual("Unexpected number of removed XAttrs", expectedNumToRemove
					, removedXAttrs.Count);
				VerifyXAttrsPresent(newXAttrs, numExpectedXAttrs);
				existingXAttrs = newXAttrs;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestXAttrMultiAddRemoveErrors()
		{
			// Test that the same XAttr can not be multiset twice
			IList<XAttr> existingXAttrs = Lists.NewArrayList();
			IList<XAttr> toAdd = Lists.NewArrayList();
			toAdd.AddItem(generatedXAttrs[0]);
			toAdd.AddItem(generatedXAttrs[1]);
			toAdd.AddItem(generatedXAttrs[2]);
			toAdd.AddItem(generatedXAttrs[0]);
			try
			{
				FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, toAdd, EnumSet.Of(XAttrSetFlag
					.Create));
				NUnit.Framework.Assert.Fail("Specified the same xattr to be set twice");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot specify the same " + "XAttr to be set"
					, e);
			}
			// Test that CREATE and REPLACE flags are obeyed
			toAdd.Remove(generatedXAttrs[0]);
			existingXAttrs.AddItem(generatedXAttrs[0]);
			try
			{
				FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, toAdd, EnumSet.Of(XAttrSetFlag
					.Create));
				NUnit.Framework.Assert.Fail("Set XAttr that is already set without REPLACE flag");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("already exists", e);
			}
			try
			{
				FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, toAdd, EnumSet.Of(XAttrSetFlag
					.Replace));
				NUnit.Framework.Assert.Fail("Set XAttr that does not exist without the CREATE flag"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("does not exist", e);
			}
			// Sanity test for CREATE
			toAdd.Remove(generatedXAttrs[0]);
			IList<XAttr> newXAttrs = FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, toAdd
				, EnumSet.Of(XAttrSetFlag.Create));
			NUnit.Framework.Assert.AreEqual("Unexpected toAdd size", 2, toAdd.Count);
			foreach (XAttr x in toAdd)
			{
				NUnit.Framework.Assert.IsTrue("Did not find added XAttr " + x, newXAttrs.Contains
					(x));
			}
			existingXAttrs = newXAttrs;
			// Sanity test for REPLACE
			toAdd = Lists.NewArrayList();
			for (int i = 0; i < 3; i++)
			{
				XAttr xAttr = (new XAttr.Builder()).SetNameSpace(XAttr.NameSpace.System).SetName(
					"a" + i).SetValue(new byte[] { unchecked((byte)(i * 2)) }).Build();
				toAdd.AddItem(xAttr);
			}
			newXAttrs = FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, toAdd, EnumSet.Of(
				XAttrSetFlag.Replace));
			NUnit.Framework.Assert.AreEqual("Unexpected number of new XAttrs", 3, newXAttrs.Count
				);
			for (int i_1 = 0; i_1 < 3; i_1++)
			{
				Assert.AssertArrayEquals("Unexpected XAttr value", new byte[] { unchecked((byte)(
					i_1 * 2)) }, newXAttrs[i_1].GetValue());
			}
			existingXAttrs = newXAttrs;
			// Sanity test for CREATE+REPLACE
			toAdd = Lists.NewArrayList();
			for (int i_2 = 0; i_2 < 4; i_2++)
			{
				toAdd.AddItem(generatedXAttrs[i_2]);
			}
			newXAttrs = FSDirXAttrOp.SetINodeXAttrs(fsdir, existingXAttrs, toAdd, EnumSet.Of(
				XAttrSetFlag.Create, XAttrSetFlag.Replace));
			VerifyXAttrsPresent(newXAttrs, 4);
		}

		public TestFSDirectory()
		{
			sub1 = new Path(dir, "sub1");
			file1 = new Path(sub1, "file1");
			file2 = new Path(sub1, "file2");
			sub11 = new Path(sub1, "sub11");
			file3 = new Path(sub11, "file3");
			file5 = new Path(sub1, "z_file5");
			sub2 = new Path(dir, "sub2");
			file6 = new Path(sub2, "file6");
		}
	}
}
