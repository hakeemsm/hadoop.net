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
using System.Collections.Generic;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>An implementation of the core algorithm of MergeSort.</summary>
	public class MergeSort
	{
		internal IntWritable I = new IntWritable(0);

		internal IntWritable J = new IntWritable(0);

		private IComparer<IntWritable> comparator;

		public MergeSort(IComparer<IntWritable> comparator)
		{
			//Reusable IntWritables
			//the comparator that the algo should use
			this.comparator = comparator;
		}

		public virtual void MergeSort(int[] src, int[] dest, int low, int high)
		{
			int length = high - low;
			// Insertion sort on smallest arrays
			if (length < 7)
			{
				for (int i = low; i < high; i++)
				{
					for (int j = i; j > low; j--)
					{
						I.Set(dest[j - 1]);
						J.Set(dest[j]);
						if (comparator.Compare(I, J) > 0)
						{
							Swap(dest, j, j - 1);
						}
					}
				}
				return;
			}
			// Recursively sort halves of dest into src
			int mid = (int)(((uint)(low + high)) >> 1);
			MergeSort(dest, src, low, mid);
			MergeSort(dest, src, mid, high);
			I.Set(src[mid - 1]);
			J.Set(src[mid]);
			// If list is already sorted, just copy from src to dest.  This is an
			// optimization that results in faster sorts for nearly ordered lists.
			if (comparator.Compare(I, J) <= 0)
			{
				System.Array.Copy(src, low, dest, low, length);
				return;
			}
			// Merge sorted halves (now in src) into dest
			for (int i_1 = low; i_1 < high; i_1++)
			{
				if (q < high && p < mid)
				{
					I.Set(src[p]);
					J.Set(src[q]);
				}
				if (q >= high || p < mid && comparator.Compare(I, J) <= 0)
				{
					dest[i_1] = src[p++];
				}
				else
				{
					dest[i_1] = src[q++];
				}
			}
		}

		private void Swap(int[] x, int a, int b)
		{
			int t = x[a];
			x[a] = x[b];
			x[b] = t;
		}
	}
}
