/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>An implementation of the core algorithm of QuickSort.</summary>
	public sealed class QuickSort : IndexedSorter
	{
		private static readonly IndexedSorter alt = new HeapSort();

		public QuickSort()
		{
		}

		private static void Fix(IndexedSortable s, int p, int r)
		{
			if (s.Compare(p, r) > 0)
			{
				s.Swap(p, r);
			}
		}

		/// <summary>Deepest recursion before giving up and doing a heapsort.</summary>
		/// <remarks>
		/// Deepest recursion before giving up and doing a heapsort.
		/// Returns 2 * ceil(log(n)).
		/// </remarks>
		protected internal static int GetMaxDepth(int x)
		{
			if (x <= 0)
			{
				throw new ArgumentException("Undefined for " + x);
			}
			return (32 - Extensions.NumberOfLeadingZeros(x - 1)) << 2;
		}

		/// <summary>Sort the given range of items using quick sort.</summary>
		/// <remarks>
		/// Sort the given range of items using quick sort.
		/// <inheritDoc/>
		/// If the recursion depth falls below
		/// <see cref="GetMaxDepth(int)"/>
		/// ,
		/// then switch to
		/// <see cref="HeapSort"/>
		/// .
		/// </remarks>
		public void Sort(IndexedSortable s, int p, int r)
		{
			Sort(s, p, r, null);
		}

		public void Sort(IndexedSortable s, int p, int r, Progressable rep)
		{
			SortInternal(s, p, r, rep, GetMaxDepth(r - p));
		}

		private static void SortInternal(IndexedSortable s, int p, int r, Progressable rep
			, int depth)
		{
			if (null != rep)
			{
				rep.Progress();
			}
			while (true)
			{
				if (r - p < 13)
				{
					for (int i = p; i < r; ++i)
					{
						for (int j = i; j > p && s.Compare(j - 1, j) > 0; --j)
						{
							s.Swap(j, j - 1);
						}
					}
					return;
				}
				if (--depth < 0)
				{
					// give up
					alt.Sort(s, p, r, rep);
					return;
				}
				// select, move pivot into first position
				Fix(s, (int)(((uint)(p + r)) >> 1), p);
				Fix(s, (int)(((uint)(p + r)) >> 1), r - 1);
				Fix(s, p, r - 1);
				// Divide
				int i_1 = p;
				int j_1 = r;
				int ll = p;
				int rr = r;
				int cr;
				while (true)
				{
					while (++i_1 < j_1)
					{
						if ((cr = s.Compare(i_1, p)) > 0)
						{
							break;
						}
						if (0 == cr && ++ll != i_1)
						{
							s.Swap(ll, i_1);
						}
					}
					while (--j_1 > i_1)
					{
						if ((cr = s.Compare(p, j_1)) > 0)
						{
							break;
						}
						if (0 == cr && --rr != j_1)
						{
							s.Swap(rr, j_1);
						}
					}
					if (i_1 < j_1)
					{
						s.Swap(i_1, j_1);
					}
					else
					{
						break;
					}
				}
				j_1 = i_1;
				// swap pivot- and all eq values- into position
				while (ll >= p)
				{
					s.Swap(ll--, --i_1);
				}
				while (rr < r)
				{
					s.Swap(rr++, j_1++);
				}
				// Conquer
				// Recurse on smaller interval first to keep stack shallow
				System.Diagnostics.Debug.Assert(i_1 != j_1);
				if (i_1 - p < r - j_1)
				{
					SortInternal(s, p, i_1, rep, depth);
					p = j_1;
				}
				else
				{
					SortInternal(s, j_1, r, rep, depth);
					r = i_1;
				}
			}
		}
	}
}
