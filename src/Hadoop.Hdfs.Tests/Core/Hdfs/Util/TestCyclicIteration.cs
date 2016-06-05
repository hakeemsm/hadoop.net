using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	public class TestCyclicIteration
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCyclicIteration()
		{
			for (int n = 0; n < 5; n++)
			{
				CheckCyclicIteration(n);
			}
		}

		private static void CheckCyclicIteration(int numOfElements)
		{
			//create a tree map
			NavigableMap<int, int> map = new SortedDictionary<int, int>();
			int[] integers = new int[numOfElements];
			for (int i = 0; i < integers.Length; i++)
			{
				integers[i] = 2 * i;
				map[integers[i]] = integers[i];
			}
			System.Console.Out.WriteLine("\n\nintegers=" + Arrays.AsList(integers));
			System.Console.Out.WriteLine("map=" + map);
			//try starting everywhere
			for (int start = -1; start <= 2 * integers.Length - 1; start++)
			{
				//get a cyclic iteration
				IList<int> iteration = new AList<int>();
				foreach (KeyValuePair<int, int> e in new CyclicIteration<int, int>(map, start))
				{
					iteration.AddItem(e.Key);
				}
				System.Console.Out.WriteLine("start=" + start + ", iteration=" + iteration);
				//verify results
				for (int i_1 = 0; i_1 < integers.Length; i_1++)
				{
					int j = ((start + 2) / 2 + i_1) % integers.Length;
					NUnit.Framework.Assert.AreEqual("i=" + i_1 + ", j=" + j, iteration[i_1], integers
						[j]);
				}
			}
		}
	}
}
