using Mono.Math;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	public class TestLongLong : TestCase
	{
		internal static readonly Random Ran = new Random();

		internal const long Mask = (1L << (LongLong.Size >> 1)) - 1;

		internal static long NextPositiveLong()
		{
			return Ran.NextLong() & Mask;
		}

		internal static void VerifyMultiplication(long a, long b)
		{
			LongLong ll = LongLong.Multiplication(new LongLong(), a, b);
			BigInteger bi = BigInteger.ValueOf(a).Multiply(BigInteger.ValueOf(b));
			string s = string.Format("\na = %x\nb = %x\nll= " + ll + "\nbi= " + bi.ToString(16
				) + "\n", a, b);
			//System.out.println(s);
			NUnit.Framework.Assert.AreEqual(s, bi, ll.ToBigInteger());
		}

		public virtual void TestMultiplication()
		{
			for (int i = 0; i < 100; i++)
			{
				long a = NextPositiveLong();
				long b = NextPositiveLong();
				VerifyMultiplication(a, b);
			}
			long max = long.MaxValue & Mask;
			VerifyMultiplication(max, max);
		}

		internal static void VerifyRightShift(long a, long b)
		{
			LongLong ll = new LongLong().Set(a, b);
			BigInteger bi = ll.ToBigInteger();
			for (int i = 0; i < LongLong.Size >> 1; i++)
			{
				long result = ll.ShiftRight(i) & Mask;
				long expected = bi.ShiftRight(i) & Mask;
				string s = string.Format("\na = %x\nb = %x\nll= " + ll + "\nbi= " + bi.ToString(16
					) + "\n", a, b);
				NUnit.Framework.Assert.AreEqual(s, expected, result);
			}
			string s_1 = string.Format("\na = %x\nb = %x\nll= " + ll + "\nbi= " + bi.ToString
				(16) + "\n", a, b);
			//System.out.println(s);
			NUnit.Framework.Assert.AreEqual(s_1, bi, ll.ToBigInteger());
		}

		public virtual void TestRightShift()
		{
			for (int i = 0; i < 1000; i++)
			{
				long a = NextPositiveLong();
				long b = NextPositiveLong();
				VerifyMultiplication(a, b);
			}
		}
	}
}
