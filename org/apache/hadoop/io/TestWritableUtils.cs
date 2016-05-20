using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestWritableUtils : NUnit.Framework.TestCase
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestWritableUtils
			)));

		/// <exception cref="System.IO.IOException"/>
		public static void testValue(int val, int vintlen)
		{
			org.apache.hadoop.io.DataOutputBuffer buf = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataInputBuffer inbuf = new org.apache.hadoop.io.DataInputBuffer
				();
			org.apache.hadoop.io.WritableUtils.writeVInt(buf, val);
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Value = " + val);
				org.apache.hadoop.io.BytesWritable printer = new org.apache.hadoop.io.BytesWritable
					();
				printer.set(buf.getData(), 0, buf.getLength());
				LOG.debug("Buffer = " + printer);
			}
			inbuf.reset(buf.getData(), 0, buf.getLength());
			NUnit.Framework.Assert.AreEqual(val, org.apache.hadoop.io.WritableUtils.readVInt(
				inbuf));
			NUnit.Framework.Assert.AreEqual(vintlen, buf.getLength());
			NUnit.Framework.Assert.AreEqual(vintlen, org.apache.hadoop.io.WritableUtils.getVIntSize
				(val));
			NUnit.Framework.Assert.AreEqual(vintlen, org.apache.hadoop.io.WritableUtils.decodeVIntSize
				(buf.getData()[0]));
		}

		/// <exception cref="System.IO.IOException"/>
		public static void testReadInRange(long val, int lower, int upper, bool expectSuccess
			)
		{
			org.apache.hadoop.io.DataOutputBuffer buf = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataInputBuffer inbuf = new org.apache.hadoop.io.DataInputBuffer
				();
			org.apache.hadoop.io.WritableUtils.writeVLong(buf, val);
			try
			{
				inbuf.reset(buf.getData(), 0, buf.getLength());
				long val2 = org.apache.hadoop.io.WritableUtils.readVIntInRange(inbuf, lower, upper
					);
				if (!expectSuccess)
				{
					fail("expected readVIntInRange to throw an exception");
				}
				NUnit.Framework.Assert.AreEqual(val, val2);
			}
			catch (System.IO.IOException e)
			{
				if (expectSuccess)
				{
					LOG.error("unexpected exception:", e);
					fail("readVIntInRange threw an unexpected exception");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void testVInt()
		{
			testValue(12, 1);
			testValue(127, 1);
			testValue(-112, 1);
			testValue(-113, 2);
			testValue(-128, 2);
			testValue(128, 2);
			testValue(-129, 2);
			testValue(255, 2);
			testValue(-256, 2);
			testValue(256, 3);
			testValue(-257, 3);
			testValue(65535, 3);
			testValue(-65536, 3);
			testValue(65536, 4);
			testValue(-65537, 4);
			testReadInRange(123, 122, 123, true);
			testReadInRange(123, 0, 100, false);
			testReadInRange(0, 0, 100, true);
			testReadInRange(-1, 0, 100, false);
			testReadInRange(1099511627776L, 0, int.MaxValue, false);
		}
	}
}
