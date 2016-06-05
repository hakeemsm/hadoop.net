using System.IO;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestContentSummary
	{
		// check the empty constructor correctly initialises the object
		[Fact]
		public virtual void TestConstructorEmpty()
		{
			ContentSummary contentSummary = new ContentSummary.Builder().Build();
			Assert.Equal("getLength", 0, contentSummary.GetLength());
			Assert.Equal("getFileCount", 0, contentSummary.GetFileCount());
			Assert.Equal("getDirectoryCount", 0, contentSummary.GetDirectoryCount
				());
			Assert.Equal("getQuota", -1, contentSummary.GetQuota());
			Assert.Equal("getSpaceConsumed", 0, contentSummary.GetSpaceConsumed
				());
			Assert.Equal("getSpaceQuota", -1, contentSummary.GetSpaceQuota
				());
		}

		// check the full constructor with quota information
		[Fact]
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
			Assert.Equal("getLength", length, contentSummary.GetLength());
			Assert.Equal("getFileCount", fileCount, contentSummary.GetFileCount
				());
			Assert.Equal("getDirectoryCount", directoryCount, contentSummary
				.GetDirectoryCount());
			Assert.Equal("getQuota", quota, contentSummary.GetQuota());
			Assert.Equal("getSpaceConsumed", spaceConsumed, contentSummary
				.GetSpaceConsumed());
			Assert.Equal("getSpaceQuota", spaceQuota, contentSummary.GetSpaceQuota
				());
		}

		// check the constructor with quota information
		[Fact]
		public virtual void TestConstructorNoQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).SpaceConsumed(length).Build();
			Assert.Equal("getLength", length, contentSummary.GetLength());
			Assert.Equal("getFileCount", fileCount, contentSummary.GetFileCount
				());
			Assert.Equal("getDirectoryCount", directoryCount, contentSummary
				.GetDirectoryCount());
			Assert.Equal("getQuota", -1, contentSummary.GetQuota());
			Assert.Equal("getSpaceConsumed", length, contentSummary.GetSpaceConsumed
				());
			Assert.Equal("getSpaceQuota", -1, contentSummary.GetSpaceQuota
				());
		}

		// check the write method
		/// <exception cref="System.IO.IOException"/>
		[Fact]
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
			BinaryWriter @out = Org.Mockito.Mockito.Mock<BinaryWriter>();
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
		[Fact]
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
			Assert.Equal("getLength", length, contentSummary.GetLength());
			Assert.Equal("getFileCount", fileCount, contentSummary.GetFileCount
				());
			Assert.Equal("getDirectoryCount", directoryCount, contentSummary
				.GetDirectoryCount());
			Assert.Equal("getQuota", quota, contentSummary.GetQuota());
			Assert.Equal("getSpaceConsumed", spaceConsumed, contentSummary
				.GetSpaceConsumed());
			Assert.Equal("getSpaceQuota", spaceQuota, contentSummary.GetSpaceQuota
				());
		}

		// check the header with quotas
		[Fact]
		public virtual void TestGetHeaderWithQuota()
		{
			string header = "  name quota  rem name quota     space quota " + "rem space quota  directories        files              bytes ";
			Assert.Equal(header, ContentSummary.GetHeader(true));
		}

		// check the header without quotas
		[Fact]
		public virtual void TestGetHeaderNoQuota()
		{
			string header = " directories        files              bytes ";
			Assert.Equal(header, ContentSummary.GetHeader(false));
		}

		// check the toString method with quotas
		[Fact]
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
			Assert.Equal(expected, contentSummary.ToString(true));
		}

		// check the toString method with quotas
		[Fact]
		public virtual void TestToStringNoQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			ContentSummary contentSummary = new ContentSummary.Builder().Length(length).FileCount
				(fileCount).DirectoryCount(directoryCount).Build();
			string expected = "        none             inf            none" + "             inf        33333        22222              11111 ";
			Assert.Equal(expected, contentSummary.ToString(true));
		}

		// check the toString method with quotas
		[Fact]
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
			Assert.Equal(expected, contentSummary.ToString(false));
		}

		// check the toString method (defaults to with quotas)
		[Fact]
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
			Assert.Equal(expected, contentSummary.ToString());
		}

		// check the toString method with quotas
		[Fact]
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
			Assert.Equal(expected, contentSummary.ToString(true, true));
		}

		// check the toString method with quotas
		[Fact]
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
			Assert.Equal(expected, contentSummary.ToString(false, true));
		}
	}
}
