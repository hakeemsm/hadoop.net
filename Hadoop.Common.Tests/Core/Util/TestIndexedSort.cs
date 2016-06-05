using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestIndexedSort : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void SortAllEqual(IndexedSorter sorter)
		{
			int Sample = 500;
			int[] values = new int[Sample];
			Arrays.Fill(values, 10);
			TestIndexedSort.SampleSortable s = new TestIndexedSort.SampleSortable(values);
			sorter.Sort(s, 0, Sample);
			int[] check = s.GetSorted();
			Assert.True(Arrays.ToString(values) + "\ndoesn't match\n" + Arrays
				.ToString(check), Arrays.Equals(values, check));
			// Set random min/max, re-sort.
			Random r = new Random();
			int min = r.Next(Sample);
			int max = (min + 1 + r.Next(Sample - 2)) % Sample;
			values[min] = 9;
			values[max] = 11;
			System.Console.Out.WriteLine("testAllEqual setting min/max at " + min + "/" + max
				 + "(" + sorter.GetType().FullName + ")");
			s = new TestIndexedSort.SampleSortable(values);
			sorter.Sort(s, 0, Sample);
			check = s.GetSorted();
			Arrays.Sort(values);
			Assert.True(check[0] == 9);
			Assert.True(check[Sample - 1] == 11);
			Assert.True(Arrays.ToString(values) + "\ndoesn't match\n" + Arrays
				.ToString(check), Arrays.Equals(values, check));
		}

		/// <exception cref="System.Exception"/>
		public virtual void SortSorted(IndexedSorter sorter)
		{
			int Sample = 500;
			int[] values = new int[Sample];
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			System.Console.Out.WriteLine("testSorted seed: " + seed + "(" + sorter.GetType().
				FullName + ")");
			for (int i = 0; i < Sample; ++i)
			{
				values[i] = r.Next(100);
			}
			Arrays.Sort(values);
			TestIndexedSort.SampleSortable s = new TestIndexedSort.SampleSortable(values);
			sorter.Sort(s, 0, Sample);
			int[] check = s.GetSorted();
			Assert.True(Arrays.ToString(values) + "\ndoesn't match\n" + Arrays
				.ToString(check), Arrays.Equals(values, check));
		}

		/// <exception cref="System.Exception"/>
		public virtual void SortSequential(IndexedSorter sorter)
		{
			int Sample = 500;
			int[] values = new int[Sample];
			for (int i = 0; i < Sample; ++i)
			{
				values[i] = i;
			}
			TestIndexedSort.SampleSortable s = new TestIndexedSort.SampleSortable(values);
			sorter.Sort(s, 0, Sample);
			int[] check = s.GetSorted();
			Assert.True(Arrays.ToString(values) + "\ndoesn't match\n" + Arrays
				.ToString(check), Arrays.Equals(values, check));
		}

		/// <exception cref="System.Exception"/>
		public virtual void SortSingleRecord(IndexedSorter sorter)
		{
			int Sample = 1;
			TestIndexedSort.SampleSortable s = new TestIndexedSort.SampleSortable(Sample);
			int[] values = s.GetValues();
			sorter.Sort(s, 0, Sample);
			int[] check = s.GetSorted();
			Assert.True(Arrays.ToString(values) + "\ndoesn't match\n" + Arrays
				.ToString(check), Arrays.Equals(values, check));
		}

		/// <exception cref="System.Exception"/>
		public virtual void SortRandom(IndexedSorter sorter)
		{
			int Sample = 256 * 1024;
			TestIndexedSort.SampleSortable s = new TestIndexedSort.SampleSortable(Sample);
			long seed = s.GetSeed();
			System.Console.Out.WriteLine("sortRandom seed: " + seed + "(" + sorter.GetType().
				FullName + ")");
			int[] values = s.GetValues();
			Arrays.Sort(values);
			sorter.Sort(s, 0, Sample);
			int[] check = s.GetSorted();
			Assert.True("seed: " + seed + "\ndoesn't match\n", Arrays.Equals
				(values, check));
		}

		/// <exception cref="System.Exception"/>
		public virtual void SortWritable(IndexedSorter sorter)
		{
			int Sample = 1000;
			TestIndexedSort.WritableSortable s = new TestIndexedSort.WritableSortable(Sample);
			long seed = s.GetSeed();
			System.Console.Out.WriteLine("sortWritable seed: " + seed + "(" + sorter.GetType(
				).FullName + ")");
			string[] values = s.GetValues();
			Arrays.Sort(values);
			sorter.Sort(s, 0, Sample);
			string[] check = s.GetSorted();
			Assert.True("seed: " + seed + "\ndoesn't match", Arrays.Equals(
				values, check));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuickSort()
		{
			QuickSort sorter = new QuickSort();
			SortRandom(sorter);
			SortSingleRecord(sorter);
			SortSequential(sorter);
			SortSorted(sorter);
			SortAllEqual(sorter);
			SortWritable(sorter);
			// test degenerate case for median-of-three partitioning
			// a_n, a_1, a_2, ..., a_{n-1}
			int Dsample = 500;
			int[] values = new int[Dsample];
			for (int i = 0; i < Dsample; ++i)
			{
				values[i] = i;
			}
			values[0] = values[Dsample - 1] + 1;
			TestIndexedSort.SampleSortable s = new TestIndexedSort.SampleSortable(values);
			values = s.GetValues();
			int Dss = (Dsample / 2) * (Dsample / 2);
			// Worst case is (N/2)^2 comparisons, not including those effecting
			// the median-of-three partitioning; impl should handle this case
			TestIndexedSort.MeasuredSortable m = new TestIndexedSort.MeasuredSortable(s, Dss);
			sorter.Sort(m, 0, Dsample);
			System.Console.Out.WriteLine("QuickSort degen cmp/swp: " + m.GetCmp() + "/" + m.GetSwp
				() + "(" + sorter.GetType().FullName + ")");
			Arrays.Sort(values);
			int[] check = s.GetSorted();
			Assert.True(Arrays.Equals(values, check));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHeapSort()
		{
			HeapSort sorter = new HeapSort();
			SortRandom(sorter);
			SortSingleRecord(sorter);
			SortSequential(sorter);
			SortSorted(sorter);
			SortAllEqual(sorter);
			SortWritable(sorter);
		}

		private class SampleSortable : IndexedSortable
		{
			private int[] valindex;

			private int[] valindirect;

			private int[] values;

			private readonly long seed;

			public SampleSortable()
				: this(50)
			{
			}

			public SampleSortable(int j)
			{
				// Sortables //
				Random r = new Random();
				seed = r.NextLong();
				r.SetSeed(seed);
				values = new int[j];
				valindex = new int[j];
				valindirect = new int[j];
				for (int i = 0; i < j; ++i)
				{
					valindex[i] = valindirect[i] = i;
					values[i] = r.Next(1000);
				}
			}

			public SampleSortable(int[] values)
			{
				this.values = values;
				valindex = new int[values.Length];
				valindirect = new int[values.Length];
				for (int i = 0; i < values.Length; ++i)
				{
					valindex[i] = valindirect[i] = i;
				}
				seed = 0;
			}

			public virtual long GetSeed()
			{
				return seed;
			}

			public virtual int Compare(int i, int j)
			{
				// assume positive
				return values[valindirect[valindex[i]]] - values[valindirect[valindex[j]]];
			}

			public virtual void Swap(int i, int j)
			{
				int tmp = valindex[i];
				valindex[i] = valindex[j];
				valindex[j] = tmp;
			}

			public virtual int[] GetSorted()
			{
				int[] ret = new int[values.Length];
				for (int i = 0; i < ret.Length; ++i)
				{
					ret[i] = values[valindirect[valindex[i]]];
				}
				return ret;
			}

			public virtual int[] GetValues()
			{
				int[] ret = new int[values.Length];
				System.Array.Copy(values, 0, ret, 0, values.Length);
				return ret;
			}
		}

		public class MeasuredSortable : IndexedSortable
		{
			private int comparisions;

			private int swaps;

			private readonly int maxcmp;

			private readonly int maxswp;

			private IndexedSortable s;

			public MeasuredSortable(IndexedSortable s)
				: this(s, int.MaxValue)
			{
			}

			public MeasuredSortable(IndexedSortable s, int maxcmp)
				: this(s, maxcmp, int.MaxValue)
			{
			}

			public MeasuredSortable(IndexedSortable s, int maxcmp, int maxswp)
			{
				this.s = s;
				this.maxcmp = maxcmp;
				this.maxswp = maxswp;
			}

			public virtual int GetCmp()
			{
				return comparisions;
			}

			public virtual int GetSwp()
			{
				return swaps;
			}

			public virtual int Compare(int i, int j)
			{
				Assert.True("Expected fewer than " + maxcmp + " comparisons", ++
					comparisions < maxcmp);
				return s.Compare(i, j);
			}

			public virtual void Swap(int i, int j)
			{
				Assert.True("Expected fewer than " + maxswp + " swaps", ++swaps
					 < maxswp);
				s.Swap(i, j);
			}
		}

		private class WritableSortable : IndexedSortable
		{
			private static Random r = new Random();

			private readonly int eob;

			private readonly int[] indices;

			private readonly int[] offsets;

			private readonly byte[] bytes;

			private readonly WritableComparator comparator;

			private readonly string[] check;

			private readonly long seed;

			/// <exception cref="System.IO.IOException"/>
			public WritableSortable()
				: this(100)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public WritableSortable(int j)
			{
				seed = r.NextLong();
				r.SetSeed(seed);
				Text t = new Text();
				StringBuilder sb = new StringBuilder();
				indices = new int[j];
				offsets = new int[j];
				check = new string[j];
				DataOutputBuffer dob = new DataOutputBuffer();
				for (int i = 0; i < j; ++i)
				{
					indices[i] = i;
					offsets[i] = dob.GetLength();
					GenRandom(t, r.Next(15) + 1, sb);
					t.Write(dob);
					check[i] = t.ToString();
				}
				eob = dob.GetLength();
				bytes = dob.GetData();
				comparator = WritableComparator.Get(typeof(Org.Apache.Hadoop.IO.Text));
			}

			public virtual long GetSeed()
			{
				return seed;
			}

			private static void GenRandom(Org.Apache.Hadoop.IO.Text t, int len, StringBuilder
				 sb)
			{
				sb.Length = 0;
				for (int i = 0; i < len; ++i)
				{
					sb.Append(Sharpen.Extensions.ToString(r.Next(26) + 10, 36));
				}
				t.Set(sb.ToString());
			}

			public virtual int Compare(int i, int j)
			{
				int ii = indices[i];
				int ij = indices[j];
				return comparator.Compare(bytes, offsets[ii], ((ii + 1 == indices.Length) ? eob : 
					offsets[ii + 1]) - offsets[ii], bytes, offsets[ij], ((ij + 1 == indices.Length) ? 
					eob : offsets[ij + 1]) - offsets[ij]);
			}

			public virtual void Swap(int i, int j)
			{
				int tmp = indices[i];
				indices[i] = indices[j];
				indices[j] = tmp;
			}

			public virtual string[] GetValues()
			{
				return check;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string[] GetSorted()
			{
				string[] ret = new string[indices.Length];
				Org.Apache.Hadoop.IO.Text t = new Org.Apache.Hadoop.IO.Text();
				DataInputBuffer dib = new DataInputBuffer();
				for (int i = 0; i < ret.Length; ++i)
				{
					int ii = indices[i];
					dib.Reset(bytes, offsets[ii], ((ii + 1 == indices.Length) ? eob : offsets[ii + 1]
						) - offsets[ii]);
					t.ReadFields(dib);
					ret[i] = t.ToString();
				}
				return ret;
			}
		}
	}
}
