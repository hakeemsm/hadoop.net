using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// A low memory footprint
	/// <see cref="GSet{K, E}"/>
	/// implementation,
	/// which uses an array for storing the elements
	/// and linked lists for collision resolution.
	/// No rehash will be performed.
	/// Therefore, the internal array will never be resized.
	/// This class does not support null element.
	/// This class is not thread safe.
	/// </summary>
	/// <?/>
	/// <?/>
	public class LightWeightGSet<K, E> : org.apache.hadoop.util.GSet<K, E>
		where E : K
	{
		/// <summary>
		/// Elements of
		/// <see cref="LightWeightGSet{K, E}"/>
		/// .
		/// </summary>
		public interface LinkedElement
		{
			/// <summary>Set the next element.</summary>
			void setNext(org.apache.hadoop.util.LightWeightGSet.LinkedElement next);

			/// <summary>Get the next element.</summary>
			org.apache.hadoop.util.LightWeightGSet.LinkedElement getNext();
		}

		internal const int MAX_ARRAY_LENGTH = 1 << 30;

		internal const int MIN_ARRAY_LENGTH = 1;

		/// <summary>An internal array of entries, which are the rows of the hash table.</summary>
		/// <remarks>
		/// An internal array of entries, which are the rows of the hash table.
		/// The size must be a power of two.
		/// </remarks>
		private readonly org.apache.hadoop.util.LightWeightGSet.LinkedElement[] entries;

		/// <summary>A mask for computing the array index from the hash value of an element.</summary>
		private readonly int hash_mask;

		/// <summary>The size of the set (not the entry array).</summary>
		private int size = 0;

		/// <summary>Modification version for fail-fast.</summary>
		/// <seealso cref="java.util.ConcurrentModificationException"/>
		private int modification = 0;

		/// <param name="recommended_length">Recommended size of the internal array.</param>
		public LightWeightGSet(int recommended_length)
		{
			//prevent int overflow problem
			int actual = actualArrayLength(recommended_length);
			if (LOG.isDebugEnabled())
			{
				LOG.debug("recommended=" + recommended_length + ", actual=" + actual);
			}
			entries = new org.apache.hadoop.util.LightWeightGSet.LinkedElement[actual];
			hash_mask = entries.Length - 1;
		}

		//compute actual length
		private static int actualArrayLength(int recommended)
		{
			if (recommended > MAX_ARRAY_LENGTH)
			{
				return MAX_ARRAY_LENGTH;
			}
			else
			{
				if (recommended < MIN_ARRAY_LENGTH)
				{
					return MIN_ARRAY_LENGTH;
				}
				else
				{
					int a = int.highestOneBit(recommended);
					return a == recommended ? a : a << 1;
				}
			}
		}

		public override int size()
		{
			return size;
		}

		private int getIndex(K key)
		{
			return key.GetHashCode() & hash_mask;
		}

		private E convert(org.apache.hadoop.util.LightWeightGSet.LinkedElement e)
		{
			E r = (E)e;
			return r;
		}

		public override E get(K key)
		{
			//validate key
			if (key == null)
			{
				throw new System.ArgumentNullException("key == null");
			}
			//find element
			int index = getIndex(key);
			for (org.apache.hadoop.util.LightWeightGSet.LinkedElement e = entries[index]; e !=
				 null; e = e.getNext())
			{
				if (e.Equals(key))
				{
					return convert(e);
				}
			}
			//element not found
			return null;
		}

		public override bool contains(K key)
		{
			return get(key) != null;
		}

		public override E put(E element)
		{
			//validate element
			if (element == null)
			{
				throw new System.ArgumentNullException("Null element is not supported.");
			}
			if (!(element is org.apache.hadoop.util.LightWeightGSet.LinkedElement))
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("!(element instanceof LinkedElement), element.getClass()="
					 + Sharpen.Runtime.getClassForObject(element));
			}
			org.apache.hadoop.util.LightWeightGSet.LinkedElement e = (org.apache.hadoop.util.LightWeightGSet.LinkedElement
				)element;
			//find index
			int index = getIndex(element);
			//remove if it already exists
			E existing = remove(index, element);
			//insert the element to the head of the linked list
			modification++;
			size++;
			e.setNext(entries[index]);
			entries[index] = e;
			return existing;
		}

		/// <summary>
		/// Remove the element corresponding to the key,
		/// given key.hashCode() == index.
		/// </summary>
		/// <returns>
		/// If such element exists, return it.
		/// Otherwise, return null.
		/// </returns>
		private E remove(int index, K key)
		{
			if (entries[index] == null)
			{
				return null;
			}
			else
			{
				if (entries[index].Equals(key))
				{
					//remove the head of the linked list
					modification++;
					size--;
					org.apache.hadoop.util.LightWeightGSet.LinkedElement e = entries[index];
					entries[index] = e.getNext();
					e.setNext(null);
					return convert(e);
				}
				else
				{
					//head != null and key is not equal to head
					//search the element
					org.apache.hadoop.util.LightWeightGSet.LinkedElement prev = entries[index];
					for (org.apache.hadoop.util.LightWeightGSet.LinkedElement curr = prev.getNext(); 
						curr != null; )
					{
						if (curr.Equals(key))
						{
							//found the element, remove it
							modification++;
							size--;
							prev.setNext(curr.getNext());
							curr.setNext(null);
							return convert(curr);
						}
						else
						{
							prev = curr;
							curr = curr.getNext();
						}
					}
					//element not found
					return null;
				}
			}
		}

		public override E remove(K key)
		{
			//validate key
			if (key == null)
			{
				throw new System.ArgumentNullException("key == null");
			}
			return remove(getIndex(key), key);
		}

		public virtual System.Collections.Generic.IEnumerator<E> GetEnumerator()
		{
			return new org.apache.hadoop.util.LightWeightGSet.SetIterator(this);
		}

		public override string ToString()
		{
			java.lang.StringBuilder b = new java.lang.StringBuilder(Sharpen.Runtime.getClassForObject
				(this).getSimpleName());
			b.Append("(size=").Append(size).Append(string.format(", %08x", hash_mask)).Append
				(", modification=").Append(modification).Append(", entries.length=").Append(entries
				.Length).Append(")");
			return b.ToString();
		}

		/// <summary>Print detailed information of this object.</summary>
		public virtual void printDetails(System.IO.TextWriter @out)
		{
			@out.Write(this + ", entries = [");
			for (int i = 0; i < entries.Length; i++)
			{
				if (entries[i] != null)
				{
					org.apache.hadoop.util.LightWeightGSet.LinkedElement e = entries[i];
					@out.Write("\n  " + i + ": " + e);
					for (e = e.getNext(); e != null; e = e.getNext())
					{
						@out.Write(" -> " + e);
					}
				}
			}
			@out.WriteLine("\n]");
		}

		public class SetIterator : System.Collections.Generic.IEnumerator<E>
		{
			/// <summary>The starting modification for fail-fast.</summary>
			private int iterModification = this._enclosing.modification;

			/// <summary>The current index of the entry array.</summary>
			private int index = -1;

			private org.apache.hadoop.util.LightWeightGSet.LinkedElement cur = null;

			private org.apache.hadoop.util.LightWeightGSet.LinkedElement next = this.nextNonemptyEntry
				();

			private bool trackModification = true;

			/// <summary>Find the next nonempty entry starting at (index + 1).</summary>
			private org.apache.hadoop.util.LightWeightGSet.LinkedElement nextNonemptyEntry()
			{
				for (this.index++; this.index < this._enclosing.entries.Length && this._enclosing
					.entries[this.index] == null; this.index++)
				{
				}
				return this.index < this._enclosing.entries.Length ? this._enclosing.entries[this
					.index] : null;
			}

			private void ensureNext()
			{
				if (this.trackModification && this._enclosing.modification != this.iterModification)
				{
					throw new java.util.ConcurrentModificationException("modification=" + this._enclosing
						.modification + " != iterModification = " + this.iterModification);
				}
				if (this.next != null)
				{
					return;
				}
				if (this.cur == null)
				{
					return;
				}
				this.next = this.cur.getNext();
				if (this.next == null)
				{
					this.next = this.nextNonemptyEntry();
				}
			}

			public override bool MoveNext()
			{
				this.ensureNext();
				return this.next != null;
			}

			public override E Current
			{
				get
				{
					this.ensureNext();
					if (this.next == null)
					{
						throw new System.InvalidOperationException("There are no more elements");
					}
					this.cur = this.next;
					this.next = null;
					return this._enclosing.convert(this.cur);
				}
			}

			public override void remove()
			{
				this.ensureNext();
				if (this.cur == null)
				{
					throw new System.InvalidOperationException("There is no current element " + "to remove"
						);
				}
				this._enclosing._enclosing.remove((K)this.cur);
				this.iterModification++;
				this.cur = null;
			}

			public virtual void setTrackModification(bool trackModification)
			{
				this.trackModification = trackModification;
			}

			internal SetIterator(LightWeightGSet<K, E> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly LightWeightGSet<K, E> _enclosing;
		}

		/// <summary>Let t = percentage of max memory.</summary>
		/// <remarks>
		/// Let t = percentage of max memory.
		/// Let e = round(log_2 t).
		/// Then, we choose capacity = 2^e/(size of reference),
		/// unless it is outside the close interval [1, 2^30].
		/// </remarks>
		public static int computeCapacity(double percentage, string mapName)
		{
			return computeCapacity(java.lang.Runtime.getRuntime().maxMemory(), percentage, mapName
				);
		}

		[com.google.common.annotations.VisibleForTesting]
		internal static int computeCapacity(long maxMemory, double percentage, string mapName
			)
		{
			if (percentage > 100.0 || percentage < 0.0)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Percentage " + percentage
					 + " must be greater than or equal to 0 " + " and less than or equal to 100");
			}
			if (maxMemory < 0)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Memory " + maxMemory 
					+ " must be greater than or equal to 0");
			}
			if (percentage == 0.0 || maxMemory == 0)
			{
				return 0;
			}
			//VM detection
			//See http://java.sun.com/docs/hotspot/HotSpotFAQ.html#64bit_detection
			string vmBit = Sharpen.Runtime.getProperty("sun.arch.data.model");
			//Percentage of max memory
			double percentDivisor = 100.0 / percentage;
			double percentMemory = maxMemory / percentDivisor;
			//compute capacity
			int e1 = (int)(System.Math.log(percentMemory) / System.Math.log(2.0) + 0.5);
			int e2 = e1 - ("32".Equals(vmBit) ? 2 : 3);
			int exponent = e2 < 0 ? 0 : e2 > 30 ? 30 : e2;
			int c = 1 << exponent;
			LOG.info("Computing capacity for map " + mapName);
			LOG.info("VM type       = " + vmBit + "-bit");
			LOG.info(percentage + "% max memory " + org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.long2String(maxMemory, "B", 1) + " = " + org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.long2String((long)percentMemory, "B", 1));
			LOG.info("capacity      = 2^" + exponent + " = " + c + " entries");
			return c;
		}

		public override void clear()
		{
			for (int i = 0; i < entries.Length; i++)
			{
				entries[i] = null;
			}
			size = 0;
		}
	}
}
