using System.IO;
using NUnit.Framework;
using Org.Apache.Bookkeeper.Client;
using Org.Apache.Bookkeeper.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Zookeeper;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>Unit test for the bkjm's streams</summary>
	public class TestBookKeeperEditLogStreams
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestBookKeeperEditLogStreams
			));

		private static BKJMUtil bkutil;

		private const int numBookies = 3;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupBookkeeper()
		{
			bkutil = new BKJMUtil(numBookies);
			bkutil.Start();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownBookkeeper()
		{
			bkutil.Teardown();
		}

		/// <summary>
		/// Test that bkjm will refuse open a stream on an empty
		/// ledger.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyInputStream()
		{
			ZooKeeper zk = BKJMUtil.ConnectZooKeeper();
			BookKeeper bkc = new BookKeeper(new ClientConfiguration(), zk);
			try
			{
				LedgerHandle lh = bkc.CreateLedger(BookKeeper.DigestType.Crc32, Sharpen.Runtime.GetBytesForString
					("foobar"));
				lh.Close();
				EditLogLedgerMetadata metadata = new EditLogLedgerMetadata("/foobar", HdfsConstants
					.NamenodeLayoutVersion, lh.GetId(), unchecked((int)(0x1234)));
				try
				{
					new BookKeeperEditLogInputStream(lh, metadata, -1);
					NUnit.Framework.Assert.Fail("Shouldn't get this far, should have thrown");
				}
				catch (IOException ioe)
				{
					NUnit.Framework.Assert.IsTrue(ioe.Message.Contains("Invalid first bk entry to read"
						));
				}
				metadata = new EditLogLedgerMetadata("/foobar", HdfsConstants.NamenodeLayoutVersion
					, lh.GetId(), unchecked((int)(0x1234)));
				try
				{
					new BookKeeperEditLogInputStream(lh, metadata, 0);
					NUnit.Framework.Assert.Fail("Shouldn't get this far, should have thrown");
				}
				catch (IOException ioe)
				{
					NUnit.Framework.Assert.IsTrue(ioe.Message.Contains("Invalid first bk entry to read"
						));
				}
			}
			finally
			{
				bkc.Close();
				zk.Close();
			}
		}
	}
}
