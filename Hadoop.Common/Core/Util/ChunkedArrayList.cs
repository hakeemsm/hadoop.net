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
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Simplified List implementation which stores elements as a list
	/// of chunks, each chunk having a maximum size.
	/// </summary>
	/// <remarks>
	/// Simplified List implementation which stores elements as a list
	/// of chunks, each chunk having a maximum size. This improves over
	/// using an ArrayList in that creating a large list will never require
	/// a large amount of contiguous heap space -- thus reducing the likelihood
	/// of triggering a CMS compaction pause due to heap fragmentation.
	/// The first chunks allocated are small, but each additional chunk is
	/// 50% larger than the previous, ramping up to a configurable maximum
	/// chunk size. Reasonable defaults are provided which should be a good
	/// balance between not making any large allocations while still retaining
	/// decent performance.
	/// This currently only supports a small subset of List operations --
	/// namely addition and iteration.
	/// </remarks>
	public class ChunkedArrayList<T> : AbstractList<T>
	{
		/// <summary>The chunks which make up the full list.</summary>
		private readonly IList<IList<T>> chunks = Lists.NewArrayList();

		/// <summary>Cache of the last element in the 'chunks' array above.</summary>
		/// <remarks>
		/// Cache of the last element in the 'chunks' array above.
		/// This speeds up the add operation measurably.
		/// </remarks>
		private IList<T> lastChunk = null;

		/// <summary>The capacity with which the last chunk was allocated.</summary>
		private int lastChunkCapacity;

		/// <summary>The capacity of the first chunk to allocate in a cleared list.</summary>
		private readonly int initialChunkCapacity;

		/// <summary>The maximum number of elements for any chunk.</summary>
		private readonly int maxChunkSize;

		/// <summary>Total number of elements in the list.</summary>
		private int size;

		/// <summary>
		/// Default initial size is 6 elements, since typical minimum object
		/// size is 64 bytes, and this leaves enough space for the object
		/// header.
		/// </summary>
		private const int DefaultInitialChunkCapacity = 6;

		/// <summary>
		/// Default max size is 8K elements - which, at 8 bytes per element
		/// should be about 64KB -- small enough to easily fit in contiguous
		/// free heap space even with a fair amount of fragmentation.
		/// </summary>
		private const int DefaultMaxChunkSize = 8 * 1024;

		public ChunkedArrayList()
			: this(DefaultInitialChunkCapacity, DefaultMaxChunkSize)
		{
		}

		/// <param name="initialChunkCapacity">
		/// the capacity of the first chunk to be
		/// allocated
		/// </param>
		/// <param name="maxChunkSize">the maximum size of any chunk allocated</param>
		public ChunkedArrayList(int initialChunkCapacity, int maxChunkSize)
		{
			Preconditions.CheckArgument(maxChunkSize >= initialChunkCapacity);
			this.initialChunkCapacity = initialChunkCapacity;
			this.maxChunkSize = maxChunkSize;
		}

		public override IEnumerator<T> GetEnumerator()
		{
			IEnumerator<T> it = Iterables.Concat(chunks).GetEnumerator();
			return new _IEnumerator_115(this, it);
		}

		private sealed class _IEnumerator_115 : IEnumerator<T>
		{
			public _IEnumerator_115(ChunkedArrayList<T> _enclosing, IEnumerator<T> it)
			{
				this._enclosing = _enclosing;
				this.it = it;
			}

			public override bool HasNext()
			{
				return it.HasNext();
			}

			public override T Next()
			{
				return it.Next();
			}

			public override void Remove()
			{
				it.Remove();
				this._enclosing.size--;
			}

			private readonly ChunkedArrayList<T> _enclosing;

			private readonly IEnumerator<T> it;
		}

		public override bool AddItem(T e)
		{
			if (size == int.MaxValue)
			{
				throw new RuntimeException("Can't add an additional element to the " + "list; list already has INT_MAX elements."
					);
			}
			if (lastChunk == null)
			{
				AddChunk(initialChunkCapacity);
			}
			else
			{
				if (lastChunk.Count >= lastChunkCapacity)
				{
					int newCapacity = lastChunkCapacity + (lastChunkCapacity >> 1);
					AddChunk(Math.Min(newCapacity, maxChunkSize));
				}
			}
			size++;
			return lastChunk.AddItem(e);
		}

		public override void Clear()
		{
			chunks.Clear();
			lastChunk = null;
			lastChunkCapacity = 0;
			size = 0;
		}

		private void AddChunk(int capacity)
		{
			lastChunk = Lists.NewArrayListWithCapacity(capacity);
			chunks.AddItem(lastChunk);
			lastChunkCapacity = capacity;
		}

		public override bool IsEmpty()
		{
			return size == 0;
		}

		public override int Count
		{
			get
			{
				return size;
			}
		}

		[VisibleForTesting]
		internal virtual int GetNumChunks()
		{
			return chunks.Count;
		}

		[VisibleForTesting]
		internal virtual int GetMaxChunkSize()
		{
			int size = 0;
			foreach (IList<T> chunk in chunks)
			{
				size = Math.Max(size, chunk.Count);
			}
			return size;
		}

		public override T Get(int idx)
		{
			if (idx < 0)
			{
				throw new IndexOutOfRangeException();
			}
			int @base = 0;
			IEnumerator<IList<T>> it = chunks.GetEnumerator();
			while (it.HasNext())
			{
				IList<T> list = it.Next();
				int size = list.Count;
				if (idx < @base + size)
				{
					return list[idx - @base];
				}
				@base += size;
			}
			throw new IndexOutOfRangeException();
		}
	}
}
