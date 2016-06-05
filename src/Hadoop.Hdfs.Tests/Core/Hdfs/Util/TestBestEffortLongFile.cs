using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	public class TestBestEffortLongFile
	{
		private static readonly FilePath File = new FilePath(MiniDFSCluster.GetBaseDirectory
			() + FilePath.separatorChar + "TestBestEffortLongFile");

		[SetUp]
		public virtual void Cleanup()
		{
			if (File.Exists())
			{
				NUnit.Framework.Assert.IsTrue(File.Delete());
			}
			File.GetParentFile().Mkdirs();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSet()
		{
			BestEffortLongFile f = new BestEffortLongFile(File, 12345L);
			try
			{
				// Before the file exists, should return default.
				NUnit.Framework.Assert.AreEqual(12345L, f.Get());
				// And first access should open it.
				NUnit.Framework.Assert.IsTrue(File.Exists());
				Random r = new Random();
				for (int i = 0; i < 100; i++)
				{
					long newVal = r.NextLong();
					// Changing the value should be reflected in the next get() call.
					f.Set(newVal);
					NUnit.Framework.Assert.AreEqual(newVal, f.Get());
					// And should be reflected in a new instance (ie it actually got
					// written to the file)
					BestEffortLongFile f2 = new BestEffortLongFile(File, 999L);
					try
					{
						NUnit.Framework.Assert.AreEqual(newVal, f2.Get());
					}
					finally
					{
						IOUtils.CloseStream(f2);
					}
				}
			}
			finally
			{
				IOUtils.CloseStream(f);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncatedFileReturnsDefault()
		{
			NUnit.Framework.Assert.IsTrue(File.CreateNewFile());
			NUnit.Framework.Assert.AreEqual(0, File.Length());
			BestEffortLongFile f = new BestEffortLongFile(File, 12345L);
			try
			{
				NUnit.Framework.Assert.AreEqual(12345L, f.Get());
			}
			finally
			{
				f.Close();
			}
		}
	}
}
