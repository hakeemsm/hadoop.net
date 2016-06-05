using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Hamcrest;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestEditLogFileInputStream
	{
		private static readonly byte[] FakeLogData = TestEditLog.Hadoop20SomeEdits;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadURL()
		{
			HttpURLConnection conn = Org.Mockito.Mockito.Mock<HttpURLConnection>();
			Org.Mockito.Mockito.DoReturn(new ByteArrayInputStream(FakeLogData)).When(conn).GetInputStream
				();
			Org.Mockito.Mockito.DoReturn(HttpURLConnection.HttpOk).When(conn).GetResponseCode
				();
			Org.Mockito.Mockito.DoReturn(Sharpen.Extensions.ToString(FakeLogData.Length)).When
				(conn).GetHeaderField("Content-Length");
			URLConnectionFactory factory = Org.Mockito.Mockito.Mock<URLConnectionFactory>();
			Org.Mockito.Mockito.DoReturn(conn).When(factory).OpenConnection(Org.Mockito.Mockito
				.Any<Uri>(), Matchers.AnyBoolean());
			Uri url = new Uri("http://localhost/fakeLog");
			EditLogInputStream elis = EditLogFileInputStream.FromUrl(factory, url, HdfsConstants
				.InvalidTxid, HdfsConstants.InvalidTxid, false);
			// Read the edit log and verify that we got all of the data.
			EnumMap<FSEditLogOpCodes, Holder<int>> counts = FSImageTestUtil.CountEditLogOpTypes
				(elis);
			Assert.AssertThat(counts[FSEditLogOpCodes.OpAdd].held, CoreMatchers.Is(1));
			Assert.AssertThat(counts[FSEditLogOpCodes.OpSetGenstampV1].held, CoreMatchers.Is(
				1));
			Assert.AssertThat(counts[FSEditLogOpCodes.OpClose].held, CoreMatchers.Is(1));
			// Check that length header was picked up.
			NUnit.Framework.Assert.AreEqual(FakeLogData.Length, elis.Length());
			elis.Close();
		}
	}
}
