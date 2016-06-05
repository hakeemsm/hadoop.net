using Mono.Math;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>Tests for BaileyBorweinPlouffe</summary>
	public class TestBaileyBorweinPlouffe : TestCase
	{
		public virtual void TestMod()
		{
			BigInteger Two = BigInteger.One.Add(BigInteger.One);
			for (long n = 3; n < 100; n++)
			{
				for (long e = 1; e < 100; e++)
				{
					long r = Two.ModPow(BigInteger.ValueOf(e), BigInteger.ValueOf(n));
					NUnit.Framework.Assert.AreEqual("e=" + e + ", n=" + n, r, BaileyBorweinPlouffe.Mod
						(e, n));
				}
			}
		}

		public virtual void TestHexDigit()
		{
			long[] answers = new long[] { unchecked((int)(0x43F6)), unchecked((int)(0xA308)), 
				unchecked((int)(0x29B7)), unchecked((int)(0x49F1)), unchecked((int)(0x8AC8)), unchecked(
				(int)(0x35EA)) };
			long d = 1;
			for (int i = 0; i < answers.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual("d=" + d, answers[i], BaileyBorweinPlouffe.HexDigits
					(d));
				d *= 10;
			}
			NUnit.Framework.Assert.AreEqual(unchecked((long)(0x243FL)), BaileyBorweinPlouffe.
				HexDigits(0));
		}
	}
}
