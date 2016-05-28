using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	public class TestAtomicFileOutputStream
	{
		private const string TestString = "hello world";

		private const string TestString2 = "goodbye world";

		private static readonly FilePath TestDir = PathUtils.GetTestDir(typeof(TestAtomicFileOutputStream
			));

		private static readonly FilePath DstFile = new FilePath(TestDir, "test.txt");

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void CleanupTestDir()
		{
			NUnit.Framework.Assert.IsTrue(TestDir.Exists() || TestDir.Mkdirs());
			FileUtil.FullyDeleteContents(TestDir);
		}

		/// <summary>Test case where there is no existing file</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteNewFile()
		{
			OutputStream fos = new AtomicFileOutputStream(DstFile);
			NUnit.Framework.Assert.IsFalse(DstFile.Exists());
			fos.Write(Sharpen.Runtime.GetBytesForString(TestString));
			fos.Flush();
			NUnit.Framework.Assert.IsFalse(DstFile.Exists());
			fos.Close();
			NUnit.Framework.Assert.IsTrue(DstFile.Exists());
			string readBackData = DFSTestUtil.ReadFile(DstFile);
			NUnit.Framework.Assert.AreEqual(TestString, readBackData);
		}

		/// <summary>Test case where there is no existing file</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOverwriteFile()
		{
			NUnit.Framework.Assert.IsTrue("Creating empty dst file", DstFile.CreateNewFile());
			OutputStream fos = new AtomicFileOutputStream(DstFile);
			NUnit.Framework.Assert.IsTrue("Empty file still exists", DstFile.Exists());
			fos.Write(Sharpen.Runtime.GetBytesForString(TestString));
			fos.Flush();
			// Original contents still in place
			NUnit.Framework.Assert.AreEqual(string.Empty, DFSTestUtil.ReadFile(DstFile));
			fos.Close();
			// New contents replace original file
			string readBackData = DFSTestUtil.ReadFile(DstFile);
			NUnit.Framework.Assert.AreEqual(TestString, readBackData);
		}

		/// <summary>
		/// Test case where the flush() fails at close time - make sure
		/// that we clean up after ourselves and don't touch any
		/// existing file at the destination
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailToFlush()
		{
			// Create a file at destination
			FileOutputStream fos = new FileOutputStream(DstFile);
			fos.Write(Sharpen.Runtime.GetBytesForString(TestString2));
			fos.Close();
			OutputStream failingStream = CreateFailingStream();
			failingStream.Write(Sharpen.Runtime.GetBytesForString(TestString));
			try
			{
				failingStream.Close();
				NUnit.Framework.Assert.Fail("Close didn't throw exception");
			}
			catch (IOException)
			{
			}
			// expected
			// Should not have touched original file
			NUnit.Framework.Assert.AreEqual(TestString2, DFSTestUtil.ReadFile(DstFile));
			NUnit.Framework.Assert.AreEqual("Temporary file should have been cleaned up", DstFile
				.GetName(), Joiner.On(",").Join(TestDir.List()));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailToRename()
		{
			Assume.AssumeTrue(Shell.Windows);
			OutputStream fos = null;
			try
			{
				fos = new AtomicFileOutputStream(DstFile);
				fos.Write(Sharpen.Runtime.GetBytesForString(TestString));
				FileUtil.SetWritable(TestDir, false);
				exception.Expect(typeof(IOException));
				exception.ExpectMessage("failure in native rename");
				try
				{
					fos.Close();
				}
				finally
				{
					fos = null;
				}
			}
			finally
			{
				IOUtils.Cleanup(null, fos);
				FileUtil.SetWritable(TestDir, true);
			}
		}

		/// <summary>Create a stream that fails to flush at close time</summary>
		/// <exception cref="System.IO.FileNotFoundException"/>
		private OutputStream CreateFailingStream()
		{
			return new _AtomicFileOutputStream_155(DstFile);
		}

		private sealed class _AtomicFileOutputStream_155 : AtomicFileOutputStream
		{
			public _AtomicFileOutputStream_155(FilePath baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				throw new IOException("injected failure");
			}
		}
	}
}
