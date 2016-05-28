using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestPathComponents
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBytes2ByteArray()
		{
			TestString("/");
			TestString("/file");
			TestString("/directory/");
			TestString("//");
			TestString("/dir//file");
			TestString("/dir/dir1//");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestString(string str)
		{
			string pathString = str;
			byte[][] oldPathComponents = INode.GetPathComponents(pathString);
			byte[][] newPathComponents = DFSUtil.Bytes2byteArray(Sharpen.Runtime.GetBytesForString
				(pathString, Charsets.Utf8), unchecked((byte)Path.SeparatorChar));
			if (oldPathComponents[0] == null)
			{
				NUnit.Framework.Assert.IsTrue(oldPathComponents[0] == newPathComponents[0]);
			}
			else
			{
				NUnit.Framework.Assert.IsTrue("Path components do not match for " + pathString, Arrays
					.DeepEquals(oldPathComponents, newPathComponents));
			}
		}
	}
}
