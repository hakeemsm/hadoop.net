using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestContentSummary
	{
		// check the empty constructor correctly initialises the object
		[NUnit.Framework.Test]
		public virtual void testConstructorEmpty()
		{
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().build();
			NUnit.Framework.Assert.AreEqual("getLength", 0, contentSummary.getLength());
			NUnit.Framework.Assert.AreEqual("getFileCount", 0, contentSummary.getFileCount());
			NUnit.Framework.Assert.AreEqual("getDirectoryCount", 0, contentSummary.getDirectoryCount
				());
			NUnit.Framework.Assert.AreEqual("getQuota", -1, contentSummary.getQuota());
			NUnit.Framework.Assert.AreEqual("getSpaceConsumed", 0, contentSummary.getSpaceConsumed
				());
			NUnit.Framework.Assert.AreEqual("getSpaceQuota", -1, contentSummary.getSpaceQuota
				());
		}

		// check the full constructor with quota information
		[NUnit.Framework.Test]
		public virtual void testConstructorWithQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66666;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).quota(quota
				).spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
			NUnit.Framework.Assert.AreEqual("getLength", length, contentSummary.getLength());
			NUnit.Framework.Assert.AreEqual("getFileCount", fileCount, contentSummary.getFileCount
				());
			NUnit.Framework.Assert.AreEqual("getDirectoryCount", directoryCount, contentSummary
				.getDirectoryCount());
			NUnit.Framework.Assert.AreEqual("getQuota", quota, contentSummary.getQuota());
			NUnit.Framework.Assert.AreEqual("getSpaceConsumed", spaceConsumed, contentSummary
				.getSpaceConsumed());
			NUnit.Framework.Assert.AreEqual("getSpaceQuota", spaceQuota, contentSummary.getSpaceQuota
				());
		}

		// check the constructor with quota information
		[NUnit.Framework.Test]
		public virtual void testConstructorNoQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).spaceConsumed
				(length).build();
			NUnit.Framework.Assert.AreEqual("getLength", length, contentSummary.getLength());
			NUnit.Framework.Assert.AreEqual("getFileCount", fileCount, contentSummary.getFileCount
				());
			NUnit.Framework.Assert.AreEqual("getDirectoryCount", directoryCount, contentSummary
				.getDirectoryCount());
			NUnit.Framework.Assert.AreEqual("getQuota", -1, contentSummary.getQuota());
			NUnit.Framework.Assert.AreEqual("getSpaceConsumed", length, contentSummary.getSpaceConsumed
				());
			NUnit.Framework.Assert.AreEqual("getSpaceQuota", -1, contentSummary.getSpaceQuota
				());
		}

		// check the write method
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testWrite()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66666;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).quota(quota
				).spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
			java.io.DataOutput @out = org.mockito.Mockito.mock<java.io.DataOutput>();
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(@out);
			contentSummary.write(@out);
			inOrder.verify(@out).writeLong(length);
			inOrder.verify(@out).writeLong(fileCount);
			inOrder.verify(@out).writeLong(directoryCount);
			inOrder.verify(@out).writeLong(quota);
			inOrder.verify(@out).writeLong(spaceConsumed);
			inOrder.verify(@out).writeLong(spaceQuota);
		}

		// check the readFields method
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testReadFields()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66666;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().build();
			java.io.DataInput @in = org.mockito.Mockito.mock<java.io.DataInput>();
			org.mockito.Mockito.when(@in.readLong()).thenReturn(length).thenReturn(fileCount)
				.thenReturn(directoryCount).thenReturn(quota).thenReturn(spaceConsumed).thenReturn
				(spaceQuota);
			contentSummary.readFields(@in);
			NUnit.Framework.Assert.AreEqual("getLength", length, contentSummary.getLength());
			NUnit.Framework.Assert.AreEqual("getFileCount", fileCount, contentSummary.getFileCount
				());
			NUnit.Framework.Assert.AreEqual("getDirectoryCount", directoryCount, contentSummary
				.getDirectoryCount());
			NUnit.Framework.Assert.AreEqual("getQuota", quota, contentSummary.getQuota());
			NUnit.Framework.Assert.AreEqual("getSpaceConsumed", spaceConsumed, contentSummary
				.getSpaceConsumed());
			NUnit.Framework.Assert.AreEqual("getSpaceQuota", spaceQuota, contentSummary.getSpaceQuota
				());
		}

		// check the header with quotas
		[NUnit.Framework.Test]
		public virtual void testGetHeaderWithQuota()
		{
			string header = "  name quota  rem name quota     space quota " + "rem space quota  directories        files              bytes ";
			NUnit.Framework.Assert.AreEqual(header, org.apache.hadoop.fs.ContentSummary.getHeader
				(true));
		}

		// check the header without quotas
		[NUnit.Framework.Test]
		public virtual void testGetHeaderNoQuota()
		{
			string header = " directories        files              bytes ";
			NUnit.Framework.Assert.AreEqual(header, org.apache.hadoop.fs.ContentSummary.getHeader
				(false));
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void testToStringWithQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66665;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).quota(quota
				).spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
			string expected = "       44444          -11111           66665           11110" 
				+ "        33333        22222              11111 ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.toString(true));
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void testToStringNoQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).build();
			string expected = "        none             inf            none" + "             inf        33333        22222              11111 ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.toString(true));
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void testToStringNoShowQuota()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66665;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).quota(quota
				).spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
			string expected = "       33333        22222              11111 ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.toString(false));
		}

		// check the toString method (defaults to with quotas)
		[NUnit.Framework.Test]
		public virtual void testToString()
		{
			long length = 11111;
			long fileCount = 22222;
			long directoryCount = 33333;
			long quota = 44444;
			long spaceConsumed = 55555;
			long spaceQuota = 66665;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).quota(quota
				).spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
			string expected = "       44444          -11111           66665" + "           11110        33333        22222              11111 ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.ToString());
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void testToStringHumanWithQuota()
		{
			long length = long.MaxValue;
			long fileCount = 222222222;
			long directoryCount = 33333;
			long quota = 222256578;
			long spaceConsumed = 1073741825;
			long spaceQuota = 1;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).quota(quota
				).spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
			string expected = "     212.0 M            1023               1 " + "           -1 G       32.6 K      211.9 M              8.0 E ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.toString(true, true));
		}

		// check the toString method with quotas
		[NUnit.Framework.Test]
		public virtual void testToStringHumanNoShowQuota()
		{
			long length = long.MaxValue;
			long fileCount = 222222222;
			long directoryCount = 33333;
			long quota = 222256578;
			long spaceConsumed = 55555;
			long spaceQuota = long.MaxValue;
			org.apache.hadoop.fs.ContentSummary contentSummary = new org.apache.hadoop.fs.ContentSummary.Builder
				().length(length).fileCount(fileCount).directoryCount(directoryCount).quota(quota
				).spaceConsumed(spaceConsumed).spaceQuota(spaceQuota).build();
			string expected = "      32.6 K      211.9 M              8.0 E ";
			NUnit.Framework.Assert.AreEqual(expected, contentSummary.toString(false, true));
		}
	}
}
