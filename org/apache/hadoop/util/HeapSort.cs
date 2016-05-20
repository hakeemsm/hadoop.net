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
using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>An implementation of the core algorithm of HeapSort.</summary>
	public sealed class HeapSort : org.apache.hadoop.util.IndexedSorter
	{
		public HeapSort()
		{
		}

		private static void downHeap(org.apache.hadoop.util.IndexedSortable s, int b, int
			 i, int N)
		{
			for (int idx = i << 1; idx < N; idx = i << 1)
			{
				if (idx + 1 < N && s.compare(b + idx, b + idx + 1) < 0)
				{
					if (s.compare(b + i, b + idx + 1) < 0)
					{
						s.swap(b + i, b + idx + 1);
					}
					else
					{
						return;
					}
					i = idx + 1;
				}
				else
				{
					if (s.compare(b + i, b + idx) < 0)
					{
						s.swap(b + i, b + idx);
						i = idx;
					}
					else
					{
						return;
					}
				}
			}
		}

		/// <summary>Sort the given range of items using heap sort.</summary>
		/// <remarks>
		/// Sort the given range of items using heap sort.
		/// <inheritDoc/>
		/// </remarks>
		public void sort(org.apache.hadoop.util.IndexedSortable s, int p, int r)
		{
			sort(s, p, r, null);
		}

		public void sort(org.apache.hadoop.util.IndexedSortable s, int p, int r, org.apache.hadoop.util.Progressable
			 rep)
		{
			int N = r - p;
			// build heap w/ reverse comparator, then write in-place from end
			int t = int.highestOneBit(N);
			for (int i = t; i > 1; i = (int)(((uint)i) >> 1))
			{
				for (int j = (int)(((uint)i) >> 1); j < i; ++j)
				{
					downHeap(s, p - 1, j, N + 1);
				}
				if (null != rep)
				{
					rep.progress();
				}
			}
			for (int i_1 = r - 1; i_1 > p; --i_1)
			{
				s.swap(p, i_1);
				downHeap(s, p - 1, 1, i_1 - p + 1);
			}
		}
	}
}
