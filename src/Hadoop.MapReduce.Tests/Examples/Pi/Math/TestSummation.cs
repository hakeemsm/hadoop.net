using System.Collections.Generic;
using Mono.Math;
using NUnit.Framework;
using Org.Apache.Hadoop.Examples.PI;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	public class TestSummation : TestCase
	{
		internal static readonly Random Random = new Random();

		internal static readonly BigInteger Two = BigInteger.ValueOf(2);

		private static TestSummation.Summation2 NewSummation(long @base, long range, long
			 delta)
		{
			ArithmeticProgression N = new ArithmeticProgression('n', @base + 3, delta, @base 
				+ 3 + range);
			ArithmeticProgression E = new ArithmeticProgression('e', @base + range, -delta, @base
				);
			return new TestSummation.Summation2(N, E);
		}

		private static void RunTestSubtract(Summation sigma, IList<Summation> diff)
		{
			//    Util.out.println("diff=" + diff);
			IList<Container<Summation>> tmp = new AList<Container<Summation>>(diff.Count);
			foreach (Summation s in diff)
			{
				tmp.AddItem(s);
			}
			IList<Summation> a = sigma.RemainingTerms(tmp);
			//    Util.out.println("a   =" + a);
			Sharpen.Collections.AddAll(a, diff);
			foreach (Summation s_1 in a)
			{
				s_1.Compute();
			}
			IList<Summation> combined = Util.Combine(a);
			//    Util.out.println("combined=" + combined);
			NUnit.Framework.Assert.AreEqual(1, combined.Count);
			NUnit.Framework.Assert.AreEqual(sigma, combined[0]);
		}

		public virtual void TestSubtract()
		{
			Summation sigma = NewSummation(3, 10000, 20);
			int size = 10;
			IList<Summation> parts = Arrays.AsList(sigma.Partition(size));
			parts.Sort();
			RunTestSubtract(sigma, new AList<Summation>());
			RunTestSubtract(sigma, parts);
			for (int n = 1; n < size; n++)
			{
				for (int j = 0; j < 10; j++)
				{
					IList<Summation> diff = new AList<Summation>(parts);
					for (int i = 0; i < n; i++)
					{
						diff.Remove(Random.Next(diff.Count));
					}
					///        Collections.sort(diff);
					RunTestSubtract(sigma, diff);
				}
			}
		}

		internal class Summation2 : Summation
		{
			internal Summation2(ArithmeticProgression N, ArithmeticProgression E)
				: base(N, E)
			{
			}

			internal readonly TestModular.Montgomery2 m2 = new TestModular.Montgomery2();

			internal virtual double Compute_montgomery2()
			{
				long e = E.value;
				long n = N.value;
				double s = 0;
				for (; e > E.limit; e += E.delta)
				{
					m2.Set(n);
					s = Modular.AddMod(s, m2.Mod2(e) / (double)n);
					n += N.delta;
				}
				return s;
			}

			internal virtual double Compute_modBigInteger()
			{
				long e = E.value;
				long n = N.value;
				double s = 0;
				for (; e > E.limit; e += E.delta)
				{
					s = Modular.AddMod(s, TestModular.ModBigInteger(e, n) / (double)n);
					n += N.delta;
				}
				return s;
			}

			internal virtual double Compute_modPow()
			{
				long e = E.value;
				long n = N.value;
				double s = 0;
				for (; e > E.limit; e += E.delta)
				{
					s = Modular.AddMod(s, Two.ModPow(BigInteger.ValueOf(e), BigInteger.ValueOf(n)) / 
						n);
					n += N.delta;
				}
				return s;
			}
		}

		private static void ComputeBenchmarks(TestSummation.Summation2 sigma)
		{
			Util.Timer t = new Util.Timer(false);
			t.Tick("sigma=" + sigma);
			double value = sigma.Compute();
			t.Tick("compute=" + value);
			NUnit.Framework.Assert.AreEqual(value, sigma.Compute_modular());
			t.Tick("compute_modular");
			NUnit.Framework.Assert.AreEqual(value, sigma.Compute_montgomery());
			t.Tick("compute_montgomery");
			NUnit.Framework.Assert.AreEqual(value, sigma.Compute_montgomery2());
			t.Tick("compute_montgomery2");
			NUnit.Framework.Assert.AreEqual(value, sigma.Compute_modBigInteger());
			t.Tick("compute_modBigInteger");
			NUnit.Framework.Assert.AreEqual(value, sigma.Compute_modPow());
			t.Tick("compute_modPow");
		}

		/// <summary>Benchmarks</summary>
		public static void Main(string[] args)
		{
			long delta = 1L << 4;
			long range = 1L << 20;
			for (int i = 20; i < 40; i += 2)
			{
				ComputeBenchmarks(NewSummation(1L << i, range, delta));
			}
		}
	}
}
