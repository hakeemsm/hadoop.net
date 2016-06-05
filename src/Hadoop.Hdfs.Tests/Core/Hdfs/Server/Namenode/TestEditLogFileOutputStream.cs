using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Test the EditLogFileOutputStream</summary>
	public class TestEditLogFileOutputStream
	{
		private static readonly FilePath TestDir = PathUtils.GetTestDir(typeof(TestEditLogFileOutputStream
			));

		private static readonly FilePath TestEdits = new FilePath(TestDir, "testEditLogFileOutput.log"
			);

		internal const int MinPreallocationLength = EditLogFileOutputStream.MinPreallocationLength;

		private Configuration conf;

		[BeforeClass]
		public static void DisableFsync()
		{
			// No need to fsync for the purposes of tests. This makes
			// the tests run much faster.
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		[NUnit.Framework.SetUp]
		[TearDown]
		public virtual void DeleteEditsFile()
		{
			if (TestEdits.Exists())
			{
				TestEdits.Delete();
			}
		}

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void FlushAndCheckLength(EditLogFileOutputStream elos, long expectedLength
			)
		{
			elos.SetReadyToFlush();
			elos.FlushAndSync(true);
			NUnit.Framework.Assert.AreEqual(expectedLength, elos.GetFile().Length());
		}

		/// <summary>Tests writing to the EditLogFileOutputStream.</summary>
		/// <remarks>
		/// Tests writing to the EditLogFileOutputStream. Due to preallocation, the
		/// length of the edit log will usually be longer than its valid contents.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRawWrites()
		{
			EditLogFileOutputStream elos = new EditLogFileOutputStream(conf, TestEdits, 0);
			try
			{
				byte[] small = new byte[] { 1, 2, 3, 4, 5, 8, 7 };
				elos.Create(NameNodeLayoutVersion.CurrentLayoutVersion);
				// The first (small) write we make extends the file by 1 MB due to
				// preallocation.
				elos.WriteRaw(small, 0, small.Length);
				FlushAndCheckLength(elos, MinPreallocationLength);
				// The next small write we make goes into the area that was already
				// preallocated.
				elos.WriteRaw(small, 0, small.Length);
				FlushAndCheckLength(elos, MinPreallocationLength);
				// Now we write enough bytes so that we exceed the minimum preallocated
				// length.
				int BigWriteLength = 3 * MinPreallocationLength;
				byte[] buf = new byte[4096];
				for (int i = 0; i < buf.Length; i++)
				{
					buf[i] = 0;
				}
				int total = BigWriteLength;
				while (total > 0)
				{
					int toWrite = (total > buf.Length) ? buf.Length : total;
					elos.WriteRaw(buf, 0, toWrite);
					total -= toWrite;
				}
				FlushAndCheckLength(elos, 4 * MinPreallocationLength);
			}
			finally
			{
				if (elos != null)
				{
					elos.Close();
				}
			}
		}

		/// <summary>
		/// Tests EditLogFileOutputStream doesn't throw NullPointerException on
		/// close/abort sequence.
		/// </summary>
		/// <remarks>
		/// Tests EditLogFileOutputStream doesn't throw NullPointerException on
		/// close/abort sequence. See HDFS-2011.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogFileOutputStreamCloseAbort()
		{
			// abort after a close should just ignore
			EditLogFileOutputStream editLogStream = new EditLogFileOutputStream(conf, TestEdits
				, 0);
			editLogStream.Close();
			editLogStream.Abort();
		}

		/// <summary>
		/// Tests EditLogFileOutputStream doesn't throw NullPointerException on
		/// close/close sequence.
		/// </summary>
		/// <remarks>
		/// Tests EditLogFileOutputStream doesn't throw NullPointerException on
		/// close/close sequence. See HDFS-2011.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogFileOutputStreamCloseClose()
		{
			// close after a close should result in an IOE
			EditLogFileOutputStream editLogStream = new EditLogFileOutputStream(conf, TestEdits
				, 0);
			editLogStream.Close();
			try
			{
				editLogStream.Close();
			}
			catch (IOException ioe)
			{
				string msg = StringUtils.StringifyException(ioe);
				NUnit.Framework.Assert.IsTrue(msg, msg.Contains("Trying to use aborted output stream"
					));
			}
		}

		/// <summary>
		/// Tests EditLogFileOutputStream doesn't throw NullPointerException on being
		/// abort/abort sequence.
		/// </summary>
		/// <remarks>
		/// Tests EditLogFileOutputStream doesn't throw NullPointerException on being
		/// abort/abort sequence. See HDFS-2011.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogFileOutputStreamAbortAbort()
		{
			// abort after a close should just ignore
			EditLogFileOutputStream editLogStream = null;
			try
			{
				editLogStream = new EditLogFileOutputStream(conf, TestEdits, 0);
				editLogStream.Abort();
				editLogStream.Abort();
			}
			finally
			{
				IOUtils.Cleanup(null, editLogStream);
			}
		}
	}
}
