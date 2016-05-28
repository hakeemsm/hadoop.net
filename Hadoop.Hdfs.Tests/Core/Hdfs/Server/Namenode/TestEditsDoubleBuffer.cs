using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestEditsDoubleBuffer
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDoubleBuffer()
		{
			EditsDoubleBuffer buf = new EditsDoubleBuffer(1024);
			NUnit.Framework.Assert.IsTrue(buf.IsFlushed());
			byte[] data = new byte[100];
			buf.WriteRaw(data, 0, data.Length);
			NUnit.Framework.Assert.AreEqual("Should count new data correctly", data.Length, buf
				.CountBufferedBytes());
			NUnit.Framework.Assert.IsTrue("Writing to current buffer should not affect flush state"
				, buf.IsFlushed());
			// Swap the buffers
			buf.SetReadyToFlush();
			NUnit.Framework.Assert.AreEqual("Swapping buffers should still count buffered bytes"
				, data.Length, buf.CountBufferedBytes());
			NUnit.Framework.Assert.IsFalse(buf.IsFlushed());
			// Flush to a stream
			DataOutputBuffer outBuf = new DataOutputBuffer();
			buf.FlushTo(outBuf);
			NUnit.Framework.Assert.AreEqual(data.Length, outBuf.GetLength());
			NUnit.Framework.Assert.IsTrue(buf.IsFlushed());
			NUnit.Framework.Assert.AreEqual(0, buf.CountBufferedBytes());
			// Write some more
			buf.WriteRaw(data, 0, data.Length);
			NUnit.Framework.Assert.AreEqual("Should count new data correctly", data.Length, buf
				.CountBufferedBytes());
			buf.SetReadyToFlush();
			buf.FlushTo(outBuf);
			NUnit.Framework.Assert.AreEqual(data.Length * 2, outBuf.GetLength());
			NUnit.Framework.Assert.AreEqual(0, buf.CountBufferedBytes());
			outBuf.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ShouldFailToCloseWhenUnflushed()
		{
			EditsDoubleBuffer buf = new EditsDoubleBuffer(1024);
			buf.WriteRaw(new byte[1], 0, 1);
			try
			{
				buf.Close();
				NUnit.Framework.Assert.Fail("Did not fail to close with unflushed data");
			}
			catch (IOException ioe)
			{
				if (!ioe.ToString().Contains("still to be flushed"))
				{
					throw;
				}
			}
		}
	}
}
