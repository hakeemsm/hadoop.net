using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestIFileStreams : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestIFileStream()
		{
			int Dlen = 100;
			DataOutputBuffer dob = new DataOutputBuffer(Dlen + 4);
			IFileOutputStream ifos = new IFileOutputStream(dob);
			for (int i = 0; i < Dlen; ++i)
			{
				ifos.Write(i);
			}
			ifos.Close();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(dob.GetData(), Dlen + 4);
			IFileInputStream ifis = new IFileInputStream(dib, 104, new Configuration());
			for (int i_1 = 0; i_1 < Dlen; ++i_1)
			{
				NUnit.Framework.Assert.AreEqual(i_1, ifis.Read());
			}
			ifis.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBadIFileStream()
		{
			int Dlen = 100;
			DataOutputBuffer dob = new DataOutputBuffer(Dlen + 4);
			IFileOutputStream ifos = new IFileOutputStream(dob);
			for (int i = 0; i < Dlen; ++i)
			{
				ifos.Write(i);
			}
			ifos.Close();
			DataInputBuffer dib = new DataInputBuffer();
			byte[] b = dob.GetData();
			++b[17];
			dib.Reset(b, Dlen + 4);
			IFileInputStream ifis = new IFileInputStream(dib, 104, new Configuration());
			int i_1 = 0;
			try
			{
				while (i_1 < Dlen)
				{
					if (17 == i_1)
					{
						NUnit.Framework.Assert.AreEqual(18, ifis.Read());
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(i_1, ifis.Read());
					}
					++i_1;
				}
				ifis.Close();
			}
			catch (ChecksumException)
			{
				NUnit.Framework.Assert.AreEqual("Unexpected bad checksum", Dlen - 1, i_1);
				return;
			}
			Fail("Did not detect bad data in checksum");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBadLength()
		{
			int Dlen = 100;
			DataOutputBuffer dob = new DataOutputBuffer(Dlen + 4);
			IFileOutputStream ifos = new IFileOutputStream(dob);
			for (int i = 0; i < Dlen; ++i)
			{
				ifos.Write(i);
			}
			ifos.Close();
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(dob.GetData(), Dlen + 4);
			IFileInputStream ifis = new IFileInputStream(dib, 100, new Configuration());
			int i_1 = 0;
			try
			{
				while (i_1 < Dlen - 8)
				{
					NUnit.Framework.Assert.AreEqual(i_1++, ifis.Read());
				}
				ifis.Close();
			}
			catch (ChecksumException)
			{
				NUnit.Framework.Assert.AreEqual("Checksum before close", i_1, Dlen - 8);
				return;
			}
			Fail("Did not detect bad data in checksum");
		}
	}
}
