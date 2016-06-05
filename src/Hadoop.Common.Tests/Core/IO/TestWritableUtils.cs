using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.IO
{
	public class TestWritableUtils : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestWritableUtils));

		/// <exception cref="System.IO.IOException"/>
		public static void TestValue(int val, int vintlen)
		{
			DataOutputBuffer buf = new DataOutputBuffer();
			DataInputBuffer inbuf = new DataInputBuffer();
			WritableUtils.WriteVInt(buf, val);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Value = " + val);
				BytesWritable printer = new BytesWritable();
				printer.Set(buf.GetData(), 0, buf.GetLength());
				Log.Debug("Buffer = " + printer);
			}
			inbuf.Reset(buf.GetData(), 0, buf.GetLength());
			Assert.Equal(val, WritableUtils.ReadVInt(inbuf));
			Assert.Equal(vintlen, buf.GetLength());
			Assert.Equal(vintlen, WritableUtils.GetVIntSize(val));
			Assert.Equal(vintlen, WritableUtils.DecodeVIntSize(buf.GetData
				()[0]));
		}

		/// <exception cref="System.IO.IOException"/>
		public static void TestReadInRange(long val, int lower, int upper, bool expectSuccess
			)
		{
			DataOutputBuffer buf = new DataOutputBuffer();
			DataInputBuffer inbuf = new DataInputBuffer();
			WritableUtils.WriteVLong(buf, val);
			try
			{
				inbuf.Reset(buf.GetData(), 0, buf.GetLength());
				long val2 = WritableUtils.ReadVIntInRange(inbuf, lower, upper);
				if (!expectSuccess)
				{
					Fail("expected readVIntInRange to throw an exception");
				}
				Assert.Equal(val, val2);
			}
			catch (IOException e)
			{
				if (expectSuccess)
				{
					Log.Error("unexpected exception:", e);
					Fail("readVIntInRange threw an unexpected exception");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void TestVInt()
		{
			TestValue(12, 1);
			TestValue(127, 1);
			TestValue(-112, 1);
			TestValue(-113, 2);
			TestValue(-128, 2);
			TestValue(128, 2);
			TestValue(-129, 2);
			TestValue(255, 2);
			TestValue(-256, 2);
			TestValue(256, 3);
			TestValue(-257, 3);
			TestValue(65535, 3);
			TestValue(-65536, 3);
			TestValue(65536, 4);
			TestValue(-65537, 4);
			TestReadInRange(123, 122, 123, true);
			TestReadInRange(123, 0, 100, false);
			TestReadInRange(0, 0, 100, true);
			TestReadInRange(-1, 0, 100, false);
			TestReadInRange(1099511627776L, 0, int.MaxValue, false);
		}
	}
}
