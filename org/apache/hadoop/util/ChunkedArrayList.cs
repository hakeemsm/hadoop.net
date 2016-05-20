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
	public class ChunkedArrayList<T> : java.util.AbstractList<T>
	{
		/// <summary>The chunks which make up the full list.</summary>
		private readonly System.Collections.Generic.IList<System.Collections.Generic.IList
			<T>> chunks = com.google.common.collect.Lists.newArrayList();

		/// <summary>Cache of the last element in the 'chunks' array above.</summary>
		/// <remarks>
		/// Cache of the last element in the 'chunks' array above.
		/// This speeds up the add operation measurably.
		/// </remarks>
		private System.Collections.Generic.IList<T> lastChunk = null;

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
		private const int DEFAULT_INITIAL_CHUNK_CAPACITY = 6;

		/// <summary>
		/// Default max size is 8K elements - which, at 8 bytes per element
		/// should be about 64KB -- small enough to easily fit in contiguous
		/// free heap space even with a fair amount of fragmentation.
		/// </summary>
		private const int DEFAULT_MAX_CHUNK_SIZE = 8 * 1024;

		public ChunkedArrayList()
			: this(DEFAULT_INITIAL_CHUNK_CAPACITY, DEFAULT_MAX_CHUNK_SIZE)
		{
		}

		/// <param name="initialChunkCapacity">
		/// the capacity of the first chunk to be
		/// allocated
		/// </param>
		/// <param name="maxChunkSize">the maximum size of any chunk allocated</param>
		public ChunkedArrayList(int initialChunkCapacity, int maxChunkSize)
		{
			com.google.common.@base.Preconditions.checkArgument(maxChunkSize >= initialChunkCapacity
				);
			this.initialChunkCapacity = initialChunkCapacity;
			this.maxChunkSize = maxChunkSize;
		}

		public override System.Collections.Generic.IEnumerator<T> GetEnumerator()
		{
			System.Collections.Generic.IEnumerator<T> it = com.google.common.collect.Iterables
				.concat(chunks).GetEnumerator();
			return new _IEnumerator_115(this, it);
		}

		private sealed class _IEnumerator_115 : System.Collections.Generic.IEnumerator<T>
		{
			public _IEnumerator_115(ChunkedArrayList<T> _enclosing, System.Collections.Generic.IEnumerator
				<T> it)
			{
				this._enclosing = _enclosing;
				this.it = it;
			}

			public override bool MoveNext()
			{
				return it.MoveNext();
			}

			public override T Current
			{
				get
				{
					return it.Current;
				}
			}

			public override void remove()
			{
				it.remove();
				this._enclosing.size--;
			}

			private readonly ChunkedArrayList<T> _enclosing;

			private readonly System.Collections.Generic.IEnumerator<T> it;
		}

		public override bool add(T e)
		{
			if (size == int.MaxValue)
			{
				throw new System.Exception("Can't add an additional element to the " + "list; list already has INT_MAX elements."
					);
			}
			if (lastChunk == null)
			{
				addChunk(initialChunkCapacity);
			}
			else
			{
				if (lastChunk.Count >= lastChunkCapacity)
				{
					int newCapacity = lastChunkCapacity + (lastChunkCapacity >> 1);
					addChunk(System.Math.min(newCapacity, maxChunkSize));
				}
			}
			size++;
			return lastChunk.add(e);
		}

		public override void clear()
		{
			chunks.clear();
			lastChunk = null;
			lastChunkCapacity = 0;
			size = 0;
		}

		private void addChunk(int capacity)
		{
			lastChunk = com.google.common.collect.Lists.newArrayListWithCapacity(capacity);
			chunks.add(lastChunk);
			lastChunkCapacity = capacity;
		}

		public override bool isEmpty()
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

		[com.google.common.annotations.VisibleForTesting]
		internal virtual int getNumChunks()
		{
			return chunks.Count;
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual int getMaxChunkSize()
		{
			int size = 0;
			foreach (System.Collections.Generic.IList<T> chunk in chunks)
			{
				size = System.Math.max(size, chunk.Count);
			}
			return size;
		}

		public override T get(int idx)
		{
			if (idx < 0)
			{
				throw new System.IndexOutOfRangeException();
			}
			int @base = 0;
			System.Collections.Generic.IEnumerator<System.Collections.Generic.IList<T>> it = 
				chunks.GetEnumerator();
			while (it.MoveNext())
			{
				System.Collections.Generic.IList<T> list = it.Current;
				int size = list.Count;
				if (idx < @base + size)
				{
					return list[idx - @base];
				}
				@base += size;
			}
			throw new System.IndexOutOfRangeException();
		}
	}
}
