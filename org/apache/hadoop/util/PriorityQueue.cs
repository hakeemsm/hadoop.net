using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// A PriorityQueue maintains a partial ordering of its elements such that the
	/// least element can always be found in constant time.
	/// </summary>
	/// <remarks>
	/// A PriorityQueue maintains a partial ordering of its elements such that the
	/// least element can always be found in constant time.  Put()'s and pop()'s
	/// require log(size) time.
	/// </remarks>
	public abstract class PriorityQueue<T>
	{
		private T[] heap;

		private int size;

		private int maxSize;

		/// <summary>Determines the ordering of objects in this priority queue.</summary>
		/// <remarks>
		/// Determines the ordering of objects in this priority queue.  Subclasses
		/// must define this one method.
		/// </remarks>
		protected internal abstract bool lessThan(object a, object b);

		/// <summary>Subclass constructors must call this.</summary>
		protected internal void initialize(int maxSize)
		{
			size = 0;
			int heapSize = maxSize + 1;
			heap = (T[])new object[heapSize];
			this.maxSize = maxSize;
		}

		/// <summary>Adds an Object to a PriorityQueue in log(size) time.</summary>
		/// <remarks>
		/// Adds an Object to a PriorityQueue in log(size) time.
		/// If one tries to add more objects than maxSize from initialize
		/// a RuntimeException (ArrayIndexOutOfBound) is thrown.
		/// </remarks>
		public void put(T element)
		{
			size++;
			heap[size] = element;
			upHeap();
		}

		/// <summary>
		/// Adds element to the PriorityQueue in log(size) time if either
		/// the PriorityQueue is not full, or not lessThan(element, top()).
		/// </summary>
		/// <param name="element"/>
		/// <returns>true if element is added, false otherwise.</returns>
		public virtual bool insert(T element)
		{
			if (size < maxSize)
			{
				put(element);
				return true;
			}
			else
			{
				if (size > 0 && !lessThan(element, top()))
				{
					heap[1] = element;
					adjustTop();
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		/// <summary>Returns the least element of the PriorityQueue in constant time.</summary>
		public T top()
		{
			if (size > 0)
			{
				return heap[1];
			}
			else
			{
				return null;
			}
		}

		/// <summary>
		/// Removes and returns the least element of the PriorityQueue in log(size)
		/// time.
		/// </summary>
		public T pop()
		{
			if (size > 0)
			{
				T result = heap[1];
				// save first value
				heap[1] = heap[size];
				// move last to first
				heap[size] = null;
				// permit GC of objects
				size--;
				downHeap();
				// adjust heap
				return result;
			}
			else
			{
				return null;
			}
		}

		/// <summary>Should be called when the Object at top changes values.</summary>
		/// <remarks>
		/// Should be called when the Object at top changes values.  Still log(n)
		/// worst case, but it's at least twice as fast to <pre>
		/// { pq.top().change(); pq.adjustTop(); }
		/// </pre> instead of <pre>
		/// { o = pq.pop(); o.change(); pq.push(o); }
		/// </pre>
		/// </remarks>
		public void adjustTop()
		{
			downHeap();
		}

		/// <summary>Returns the number of elements currently stored in the PriorityQueue.</summary>
		public int size()
		{
			return size;
		}

		/// <summary>Removes all entries from the PriorityQueue.</summary>
		public void clear()
		{
			for (int i = 0; i <= size; i++)
			{
				heap[i] = null;
			}
			size = 0;
		}

		private void upHeap()
		{
			int i = size;
			T node = heap[i];
			// save bottom node
			int j = (int)(((uint)i) >> 1);
			while (j > 0 && lessThan(node, heap[j]))
			{
				heap[i] = heap[j];
				// shift parents down
				i = j;
				j = (int)(((uint)j) >> 1);
			}
			heap[i] = node;
		}

		// install saved node
		private void downHeap()
		{
			int i = 1;
			T node = heap[i];
			// save top node
			int j = i << 1;
			// find smaller child
			int k = j + 1;
			if (k <= size && lessThan(heap[k], heap[j]))
			{
				j = k;
			}
			while (j <= size && lessThan(heap[j], node))
			{
				heap[i] = heap[j];
				// shift up child
				i = j;
				j = i << 1;
				k = j + 1;
				if (k <= size && lessThan(heap[k], heap[j]))
				{
					j = k;
				}
			}
			heap[i] = node;
		}
		// install saved node
	}
}
