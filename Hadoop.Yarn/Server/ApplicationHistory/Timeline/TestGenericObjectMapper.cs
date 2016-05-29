using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	public class TestGenericObjectMapper
	{
		[NUnit.Framework.Test]
		public virtual void TestEncoding()
		{
			TestEncoding(long.MaxValue);
			TestEncoding(long.MinValue);
			TestEncoding(0l);
			TestEncoding(128l);
			TestEncoding(256l);
			TestEncoding(512l);
			TestEncoding(-256l);
		}

		private static void TestEncoding(long l)
		{
			byte[] b = GenericObjectMapper.WriteReverseOrderedLong(l);
			NUnit.Framework.Assert.AreEqual("error decoding", l, GenericObjectMapper.ReadReverseOrderedLong
				(b, 0));
			byte[] buf = new byte[16];
			System.Array.Copy(b, 0, buf, 5, 8);
			NUnit.Framework.Assert.AreEqual("error decoding at offset", l, GenericObjectMapper
				.ReadReverseOrderedLong(buf, 5));
			if (l > long.MinValue)
			{
				byte[] a = GenericObjectMapper.WriteReverseOrderedLong(l - 1);
				NUnit.Framework.Assert.AreEqual("error preserving ordering", 1, WritableComparator
					.CompareBytes(a, 0, a.Length, b, 0, b.Length));
			}
			if (l < long.MaxValue)
			{
				byte[] c = GenericObjectMapper.WriteReverseOrderedLong(l + 1);
				NUnit.Framework.Assert.AreEqual("error preserving ordering", 1, WritableComparator
					.CompareBytes(b, 0, b.Length, c, 0, c.Length));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Verify(object o)
		{
			NUnit.Framework.Assert.AreEqual(o, GenericObjectMapper.Read(GenericObjectMapper.Write
				(o)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValueTypes()
		{
			Verify(int.MaxValue);
			Verify(int.MinValue);
			NUnit.Framework.Assert.AreEqual(int.MaxValue, GenericObjectMapper.Read(GenericObjectMapper
				.Write((long)int.MaxValue)));
			NUnit.Framework.Assert.AreEqual(int.MinValue, GenericObjectMapper.Read(GenericObjectMapper
				.Write((long)int.MinValue)));
			Verify((long)int.MaxValue + 1l);
			Verify((long)int.MinValue - 1l);
			Verify(long.MaxValue);
			Verify(long.MinValue);
			NUnit.Framework.Assert.AreEqual(42, GenericObjectMapper.Read(GenericObjectMapper.
				Write(42l)));
			Verify(42);
			Verify(1.23);
			Verify("abc");
			Verify(true);
			IList<string> list = new AList<string>();
			list.AddItem("123");
			list.AddItem("abc");
			Verify(list);
			IDictionary<string, string> map = new Dictionary<string, string>();
			map["k1"] = "v1";
			map["k2"] = "v2";
			Verify(map);
		}
	}
}
