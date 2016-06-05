using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Tests NameNode interaction for all XAttr APIs.</summary>
	/// <remarks>
	/// Tests NameNode interaction for all XAttr APIs.
	/// This test suite covers restarting NN, saving new checkpoint,
	/// and also includes test of xattrs for symlinks.
	/// </remarks>
	public class TestNameNodeXAttr : FSXAttrBaseTest
	{
		private static readonly Path linkParent = new Path("/symdir1");

		private static readonly Path targetParent = new Path("/symdir2");

		private static readonly Path link = new Path(linkParent, "link");

		private static readonly Path target = new Path(targetParent, "target");

		/// <exception cref="System.Exception"/>
		public virtual void TestXAttrSymlinks()
		{
			fs.Mkdirs(linkParent);
			fs.Mkdirs(targetParent);
			DFSTestUtil.CreateFile(fs, target, 1024, (short)3, unchecked((long)(0xBEEFl)));
			fs.CreateSymlink(target, link, false);
			fs.SetXAttr(target, name1, value1);
			fs.SetXAttr(target, name2, value2);
			IDictionary<string, byte[]> xattrs = fs.GetXAttrs(link);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			fs.SetXAttr(link, name3, null);
			xattrs = fs.GetXAttrs(target);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 3);
			Assert.AssertArrayEquals(value1, xattrs[name1]);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			Assert.AssertArrayEquals(new byte[0], xattrs[name3]);
			fs.RemoveXAttr(link, name1);
			xattrs = fs.GetXAttrs(target);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 2);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			Assert.AssertArrayEquals(new byte[0], xattrs[name3]);
			fs.RemoveXAttr(target, name3);
			xattrs = fs.GetXAttrs(link);
			NUnit.Framework.Assert.AreEqual(xattrs.Count, 1);
			Assert.AssertArrayEquals(value2, xattrs[name2]);
			fs.Delete(linkParent, true);
			fs.Delete(targetParent, true);
		}
	}
}
