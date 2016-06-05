using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;

using Reflect;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Unit test to verify that the pure-Java CRC32 algorithm gives
	/// the same results as the built-in implementation.
	/// </summary>
	public class TestPureJavaCrc32
	{
		private readonly CRC32 theirs = new CRC32();

		private readonly PureJavaCrc32 ours = new PureJavaCrc32();

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCorrectness()
		{
			CheckSame();
			theirs.Update(104);
			ours.Update(104);
			CheckSame();
			CheckOnBytes(new byte[] { 40, 60, 97, unchecked((byte)(-70)) }, false);
			CheckOnBytes(Runtime.GetBytesForString("hello world!", "UTF-8"), false);
			for (int i = 0; i < 10000; i++)
			{
				byte[] randomBytes = new byte[new Random().Next(2048)];
				new Random().NextBytes(randomBytes);
				CheckOnBytes(randomBytes, false);
			}
		}

		private void CheckOnBytes(byte[] bytes, bool print)
		{
			theirs.Reset();
			ours.Reset();
			CheckSame();
			for (int i = 0; i < bytes.Length; i++)
			{
				ours.Update(bytes[i]);
				theirs.Update(bytes[i]);
				CheckSame();
			}
			if (print)
			{
				System.Console.Out.WriteLine("theirs:\t" + long.ToHexString(theirs.GetValue()) + 
					"\nours:\t" + long.ToHexString(ours.GetValue()));
			}
			theirs.Reset();
			ours.Reset();
			ours.Update(bytes, 0, bytes.Length);
			theirs.Update(bytes, 0, bytes.Length);
			if (print)
			{
				System.Console.Out.WriteLine("theirs:\t" + long.ToHexString(theirs.GetValue()) + 
					"\nours:\t" + long.ToHexString(ours.GetValue()));
			}
			CheckSame();
			if (bytes.Length >= 10)
			{
				ours.Update(bytes, 5, 5);
				theirs.Update(bytes, 5, 5);
				CheckSame();
			}
		}

		private void CheckSame()
		{
			Assert.Equal(theirs.GetValue(), ours.GetValue());
		}

		/// <summary>
		/// Generate a table to perform checksums based on the same CRC-32 polynomial
		/// that java.util.zip.CRC32 uses.
		/// </summary>
		public class Table
		{
			private readonly int[][] tables;

			private Table(int nBits, int nTables, long polynomial)
			{
				tables = new int[nTables][];
				int size = 1 << nBits;
				for (int i = 0; i < tables.Length; i++)
				{
					tables[i] = new int[size];
				}
				//compute the first table
				int[] first = tables[0];
				for (int i_1 = 0; i_1 < first.Length; i_1++)
				{
					int crc = i_1;
					for (int j = 0; j < nBits; j++)
					{
						if ((crc & 1) == 1)
						{
							crc = (int)(((uint)crc) >> 1);
							crc ^= polynomial;
						}
						else
						{
							crc = (int)(((uint)crc) >> 1);
						}
					}
					first[i_1] = crc;
				}
				//compute the remaining tables
				int mask = first.Length - 1;
				for (int j_1 = 1; j_1 < tables.Length; j_1++)
				{
					int[] previous = tables[j_1 - 1];
					int[] current = tables[j_1];
					for (int i_2 = 0; i_2 < current.Length; i_2++)
					{
						current[i_2] = ((int)(((uint)previous[i_2]) >> nBits)) ^ first[previous[i_2] & mask
							];
					}
				}
			}

			internal virtual string[] ToStrings(string nameformat)
			{
				string[] s = new string[tables.Length];
				for (int j = 0; j < tables.Length; j++)
				{
					int[] t = tables[j];
					StringBuilder b = new StringBuilder();
					b.Append(string.Format("    /* " + nameformat + " */", j));
					for (int i = 0; i < t.Length; )
					{
						b.Append("\n    ");
						for (int k = 0; k < 4; k++)
						{
							b.Append(string.Format("0x%08X, ", t[i++]));
						}
					}
					s[j] = b.ToString();
				}
				return s;
			}

			public override string ToString()
			{
				StringBuilder b = new StringBuilder();
				string tableFormat = string.Format("T%d_", Extensions.NumberOfTrailingZeros
					(tables[0].Length)) + "%d";
				string startFormat = "  private static final int " + tableFormat + "_start = %d*256;";
				for (int j = 0; j < tables.Length; j++)
				{
					b.Append(string.Format(startFormat, j, j));
					b.Append("\n");
				}
				b.Append("  private static final int[] T = new int[] {");
				foreach (string s in ToStrings(tableFormat))
				{
					b.Append("\n");
					b.Append(s);
				}
				Runtime.SetCharAt(b, b.Length - 2, '\n');
				b.Append(" };\n");
				return b.ToString();
			}

			/// <summary>Generate CRC-32 lookup tables</summary>
			/// <exception cref="System.IO.FileNotFoundException"/>
			public static void Main(string[] args)
			{
				if (args.Length != 1)
				{
					System.Console.Error.WriteLine("Usage: " + typeof(TestPureJavaCrc32.Table).FullName
						 + " <polynomial>");
					System.Environment.Exit(1);
				}
				long polynomial = long.Parse(args[0], 16);
				int i = 8;
				TestPureJavaCrc32.Table t = new TestPureJavaCrc32.Table(i, 16, polynomial);
				string s = t.ToString();
				System.Console.Out.WriteLine(s);
				//print to a file
				TextWriter @out = new TextWriter(new FileOutputStream("table" + i + ".txt"), true
					);
				try
				{
					@out.WriteLine(s);
				}
				finally
				{
					@out.Close();
				}
			}
		}

		/// <summary>
		/// Performance tests to compare performance of the Pure Java implementation
		/// to the built-in java.util.zip implementation.
		/// </summary>
		/// <remarks>
		/// Performance tests to compare performance of the Pure Java implementation
		/// to the built-in java.util.zip implementation. This can be run from the
		/// command line with:
		/// java -cp path/to/test/classes:path/to/common/classes \
		/// 'org.apache.hadoop.util.TestPureJavaCrc32$PerformanceTest'
		/// The output is in JIRA table format.
		/// </remarks>
		public class PerformanceTest
		{
			public const int MaxLen = 32 * 1024 * 1024;

			public const int BytesPerSize = MaxLen * 4;

			internal static readonly Type zip = typeof(CRC32);

			internal static readonly IList<Type> Crcs = new AList<Type>();

			static PerformanceTest()
			{
				// up to 32MB chunks
				Crcs.AddItem(zip);
				Crcs.AddItem(typeof(PureJavaCrc32));
			}

			/// <exception cref="System.Exception"/>
			public static void Main(string[] args)
			{
				PrintSystemProperties(System.Console.Out);
				DoBench(Crcs, System.Console.Out);
			}

			private static void PrintCell(string s, int width, TextWriter @out)
			{
				int w = s.Length > width ? s.Length : width;
				@out.Printf(" %" + w + "s |", s);
			}

			/// <exception cref="System.Exception"/>
			private static void DoBench(IList<Type> crcs, TextWriter @out)
			{
				byte[] bytes = new byte[MaxLen];
				new Random().NextBytes(bytes);
				// Print header
				@out.Printf("\nPerformance Table (The unit is MB/sec; #T = #Theads)\n");
				// Warm up implementations to get jit going.
				foreach (Type c in crcs)
				{
					DoBench(c, 1, bytes, 2);
					DoBench(c, 1, bytes, 2101);
				}
				// Test on a variety of sizes with different number of threads
				for (int size = 32; size <= MaxLen; size <<= 1)
				{
					DoBench(crcs, bytes, size, @out);
				}
			}

			/// <exception cref="System.Exception"/>
			private static void DoBench(IList<Type> crcs, byte[] bytes, int size, TextWriter 
				@out)
			{
				string numBytesStr = " #Bytes ";
				string numThreadsStr = "#T";
				string diffStr = "% diff";
				@out.Write('|');
				PrintCell(numBytesStr, 0, @out);
				PrintCell(numThreadsStr, 0, @out);
				for (int i = 0; i < crcs.Count; i++)
				{
					Type c = crcs[i];
					@out.Write('|');
					PrintCell(c.Name, 8, @out);
					for (int j = 0; j < i; j++)
					{
						PrintCell(diffStr, diffStr.Length, @out);
					}
				}
				@out.Printf("\n");
				for (int numThreads = 1; numThreads <= 16; numThreads <<= 1)
				{
					@out.Printf("|");
					PrintCell(size.ToString(), numBytesStr.Length, @out);
					PrintCell(numThreads.ToString(), numThreadsStr.Length, @out);
					TestPureJavaCrc32.PerformanceTest.BenchResult expected = null;
					IList<TestPureJavaCrc32.PerformanceTest.BenchResult> previous = new AList<TestPureJavaCrc32.PerformanceTest.BenchResult
						>();
					foreach (Type c in crcs)
					{
						System.GC.Collect();
						TestPureJavaCrc32.PerformanceTest.BenchResult result = DoBench(c, numThreads, bytes
							, size);
						PrintCell(string.Format("%9.1f", result.mbps), c.Name.Length + 1, @out);
						//check result
						if (c == zip)
						{
							expected = result;
						}
						else
						{
							if (expected == null)
							{
								throw new RuntimeException("The first class is " + c.FullName + " but not " + zip
									.FullName);
							}
							else
							{
								if (result.value != expected.value)
								{
									throw new RuntimeException(c + " has bugs!");
								}
							}
						}
						//compare result with previous
						foreach (TestPureJavaCrc32.PerformanceTest.BenchResult p in previous)
						{
							double diff = (result.mbps - p.mbps) / p.mbps * 100;
							PrintCell(string.Format("%5.1f%%", diff), diffStr.Length, @out);
						}
						previous.AddItem(result);
					}
					@out.Printf("\n");
				}
			}

			/// <exception cref="System.Exception"/>
			private static TestPureJavaCrc32.PerformanceTest.BenchResult DoBench(Type clazz, 
				int numThreads, byte[] bytes, int size)
			{
				Thread[] threads = new Thread[numThreads];
				TestPureJavaCrc32.PerformanceTest.BenchResult[] results = new TestPureJavaCrc32.PerformanceTest.BenchResult
					[threads.Length];
				{
					int trials = BytesPerSize / size;
					double mbProcessed = trials * size / 1024.0 / 1024.0;
					Constructor<Checksum> ctor = clazz.GetConstructor();
					for (int i = 0; i < threads.Length; i++)
					{
						int index = i;
						threads[i] = new _Thread_327(ctor, trials, bytes, size, results, index, mbProcessed
							);
					}
				}
				for (int i_1 = 0; i_1 < threads.Length; i_1++)
				{
					threads[i_1].Start();
				}
				for (int i_2 = 0; i_2 < threads.Length; i_2++)
				{
					threads[i_2].Join();
				}
				long expected = results[0].value;
				double sum = results[0].mbps;
				for (int i_3 = 1; i_3 < results.Length; i_3++)
				{
					if (results[i_3].value != expected)
					{
						throw new Exception(clazz.Name + " results not matched.");
					}
					sum += results[i_3].mbps;
				}
				return new TestPureJavaCrc32.PerformanceTest.BenchResult(expected, sum / results.
					Length);
			}

			private sealed class _Thread_327 : Thread
			{
				public _Thread_327(Constructor<Checksum> ctor, int trials, byte[] bytes, int size
					, TestPureJavaCrc32.PerformanceTest.BenchResult[] results, int index, double mbProcessed
					)
				{
					this.ctor = ctor;
					this.trials = trials;
					this.bytes = bytes;
					this.size = size;
					this.results = results;
					this.index = index;
					this.mbProcessed = mbProcessed;
					this.crc = ctor.NewInstance();
				}

				internal readonly Checksum crc;

				public override void Run()
				{
					long st = Runtime.NanoTime();
					this.crc.Reset();
					for (int i = 0; i < trials; i++)
					{
						this.crc.Update(bytes, 0, size);
					}
					long et = Runtime.NanoTime();
					double secsElapsed = (et - st) / 1000000000.0d;
					results[index] = new TestPureJavaCrc32.PerformanceTest.BenchResult(this.crc.GetValue
						(), mbProcessed / secsElapsed);
				}

				private readonly Constructor<Checksum> ctor;

				private readonly int trials;

				private readonly byte[] bytes;

				private readonly int size;

				private readonly TestPureJavaCrc32.PerformanceTest.BenchResult[] results;

				private readonly int index;

				private readonly double mbProcessed;
			}

			private class BenchResult
			{
				/// <summary>CRC value</summary>
				internal readonly long value;

				/// <summary>Speed (MB per second)</summary>
				internal readonly double mbps;

				internal BenchResult(long value, double mbps)
				{
					this.value = value;
					this.mbps = mbps;
				}
			}

			private static void PrintSystemProperties(TextWriter @out)
			{
				string[] names = new string[] { "java.version", "java.runtime.name", "java.runtime.version"
					, "java.vm.version", "java.vm.vendor", "java.vm.name", "java.vm.specification.version"
					, "java.specification.version", "os.arch", "os.name", "os.version" };
				Properties p = Runtime.GetProperties();
				foreach (string n in names)
				{
					@out.WriteLine(n + " = " + p.GetProperty(n));
				}
			}
		}
	}
}
