using System.IO;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestContentSummary
	{
		// check the empty constructor correctly initialises the object
		[NUnit.Framework.Test]
		public virtual void TestConstructorEmpty()
		{
			ContentSummary contentSummary = new ContentSummary.Builder().Build();
			NUnit.Framework.Assert.AreEqual("getLength", 0, contentSummary.GetLength());
			NUnit.Framework.Assert.AreEqual("getFileCount", 0, contentSummary.GetFileCount());
			NUnit.Framework.Assert.AreEqual("getDirectoryCount", 0, contentSummary.GetDirectoryCount
				());
			NUnit.Framework.Assert.AreEqual("getQuota", -1, contentSummary.GetQuota());
			NUnit.Framework.Assert.AreEqual("getSpaceConsumed", 0, contentSummary.GetSpaceConsumed
				());
			NUnit.Framework.Assert.AreEqual("getSpaceQuota", -1, contentSummary.GetSpaceQuota
				());
		}

		// check the full constructor with quota information
		[NUnit.Framework.Test]
		public virtual void TestConstructorWithQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66666;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Quota(quota).SpaceConsumed(spaceConsumed
				).SpaceQuota(spaceQuota).Build();
			NUnit.Framework.Assert.AreEqual("getLength", length, contentSummary.GetLength());
			NUnit.Framework.Assert.AreEqual("getFileCount", fileCount, contentSummary.GetFileCount
				());
			NUnit.Framework.Assert.AreEqual("getDirectoryCount", directoryCount, contentSummary
				.GetDirectoryCount());
			NUnit.Framework.Assert.AreEqual("getQuota", quota, contentSummary.GetQuota());
			NUnit.Framework.Assert.AreEqual("getSpaceConsumed", spaceConsumed, contentSummary
				.GetSpaceConsumed());
			NUnit.Framework.Assert.AreEqual("getSpaceQuota", spaceQuota, contentSummary.GetSpaceQuota
				());
		}

		// check the constructor with quota information
		[NUnit.Framework.Test]
		public virtual void TestConstructorNoQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).SpaceConsumed(length).Build();
			NUnit.Framework.Assert.AreEqual("getLength", length, contentSummary.GetLength());
			NUnit.Framework.Assert.AreEqual("getFileCount", fileCount, contentSummary.GetFileCount
				());
			NUnit.Framework.Assert.AreEqual("getDirectoryCount", directoryCount, contentSummary
				.GetDirectoryCount());
			NUnit.Framework.Assert.AreEqual("getQuota", -1, contentSummary.GetQuota());
			NUnit.Framework.Assert.AreEqual("getSpaceConsumed", length, contentSummary.GetSpaceConsumed
				());
			NUnit.Framework.Assert.AreEqual("getSpaceQuota", -1, contentSummary.GetSpaceQuota
				());
		}

		// check the write method
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWrite()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66666;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Quota(quota).SpaceConsumed(spaceConsumed
				).SpaceQuota(spaceQuota).Build();
			DataOutput @out = Org.Mockito.Mockito.Mock<DataOutput>();
			InOrder inOrder = Org.Mockito.Mockito.InOrder(@out);
			contentSummary.Write(@out);
			inOrder.Verify(@out).WriteLong(length);
			inOrder.Verify(@out).WriteLong(fileCount);
			inOrder.Verify(@out).WriteLong(directoryCount);
			inOrder.Verify(@out).WriteLong(quota);
			inOrder.Verify(@out).WriteLong(spaceConsumed);
			inOrder.Verify(@out).WriteLong(spaceQuota);
		}

		// check the readFields method
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReadFields()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66666;
			ContentSummary contentSummary = new ContentSummary.Builder().Build();
			BinaryReader @in = Org.Mockito.Mockito.Mock<BinaryReader>();
			Org.Mockito.Mockito.When(@in.ReadLong()).ThenReturn(length).ThenReturn(fileCount)
				.ThenReturn(directoryCount).ThenReturn(quota).ThenReturn(spaceConsumed).ThenReturn
				(spaceQuota);
			contentSummary.ReadFields(@in);
			NUnit.Framework.Assert.AreEqual("getLength", length, contentSummary.GetLength());
			NUnit.Framework.Assert.AreEqual("getFileCount", fileCount, contentSummary.GetFileCount
				());
			NUnit.Framework.Assert.AreEqual("getDirectoryCount", directoryCount, contentSummary
				.GetDirectoryCount());
			NUnit.Framework.Assert.AreEqual("getQuota", quota, contentSummary.GetQuota());
			NUnit.Framework.Assert.AreEqual("getSpaceConsumed", spaceConsumed, contentSummary
				.GetSpaceConsumed());
			NUnit.Framework.Assert.AreEqual("getSpaceQuota", spaceQuota, contentSummary.GetSpaceQuota
				());
		}

		// check the header with quotas
		[NUnit.Framework.Test]
		public virtual void TestGetHeaderWithQuota()
		{
			string header = "  name quota  rem name quota     space quota " + "rem space quota  directories        files              bytes ";
			NUnit.Framework.Assert.AreEqual(header, ContentSummary.GetHeader(true));
		}

		// check the header without quotas
		[NUnit.Framework.Test]
		public virtual void TestGetHeaderNoQuota()
		{
			string header = " directories        files              bytes ";
			NUnit.Framework.Assert.AreEqual(header, ContentSummary.GetHeader(false));
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void TestToStringWithQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66665;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Quota(quota).SpaceConsumed(spaceConsumed
				).SpaceQuota(spaceQuota).Build();
			string expected = "       44444          -11111           66665           11110" 
				+ "        33333        22222              11111 ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.ToString(true));
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void TestToStringNoQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Build();
			string expected = "        none             inf            none" + "             inf        33333        22222              11111 ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.ToString(true));
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void TestToStringNoShowQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66665;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Quota(quota).SpaceConsumed(spaceConsumed
				).SpaceQuota(spaceQuota).Build();
			string expected = "       33333        22222              11111 ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.ToString(false));
		}

		// check the toString method (defaults to with quotas)
		[NUnit.Framework.Test]
		public virtual void TestToString()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66665;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Quota(quota).SpaceConsumed(spaceConsumed
				).SpaceQuota(spaceQuota).Build();
			string expected = "       44444          -11111           66665" + "           11110        33333        22222              11111 ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.ToString());
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void TestToStringHumanWithQuota()
		{
			long length = long.MaxValue;
			long fileCount = 222222222;
			long directoryCount = 33333;
			long quota = 222256578;
			long spaceConsumed = 1073741825;
			long spaceQuota = 1;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Quota(quota).SpaceConsumed(spaceConsumed
				).SpaceQuota(spaceQuota).Build();
			string expected = "     212.0 M            1023               1 " + "           -1 G       32.6 K      211.9 M              8.0 E ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.ToString(true, true));
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void TestToStringHumanNoShowQuota()
		{
			long length = long.MaxValue;
			long fileCount = 222222222;
			long directoryCount = 33333;
			long quota = 222256578;
			long spaceConsumed = 55555;
			long spaceQuota = long.MaxValue;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Quota(quota).SpaceConsumed(spaceConsumed
				).SpaceQuota(spaceQuota).Build();
			string expected = "      32.6 K      211.9 M              8.0 E ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.ToString(false, true));
		}
	}
}
