using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop;
using Sharpen;

namespace Org.Apache.Hadoop.Util
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
	public class LightWeightGSet<K, E> : GSet<K, E>
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
			void SetNext(LightWeightGSet.LinkedElement next);

			/// <summary>Get the next element.</summary>
			LightWeightGSet.LinkedElement GetNext();
		}

		internal const int MaxArrayLength = 1 << 30;

		internal const int MinArrayLength = 1;

		/// <summary>An internal array of entries, which are the rows of the hash table.</summary>
		/// <remarks>
		/// An internal array of entries, which are the rows of the hash table.
		/// The size must be a power of two.
		/// </remarks>
		private readonly LightWeightGSet.LinkedElement[] entries;

		/// <summary>A mask for computing the array index from the hash value of an element.</summary>
		private readonly int hash_mask;

		/// <summary>The size of the set (not the entry array).</summary>
		private int size = 0;

		/// <summary>Modification version for fail-fast.</summary>
		/// <seealso cref="Sharpen.ConcurrentModificationException"/>
		private int modification = 0;

		/// <param name="recommended_length">Recommended size of the internal array.</param>
		public LightWeightGSet(int recommended_length)
		{
			//prevent int overflow problem
			int actual = ActualArrayLength(recommended_length);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("recommended=" + recommended_length + ", actual=" + actual);
			}
			entries = new LightWeightGSet.LinkedElement[actual];
			hash_mask = entries.Length - 1;
		}

		//compute actual length
		private static int ActualArrayLength(int recommended)
		{
			if (recommended > MaxArrayLength)
			{
				return MaxArrayLength;
			}
			else
			{
				if (recommended < MinArrayLength)
				{
					return MinArrayLength;
				}
				else
				{
					int a = int.HighestOneBit(recommended);
					return a == recommended ? a : a << 1;
				}
			}
		}

		public override int Size()
		{
			return size;
		}

		private int GetIndex(K key)
		{
			return key.GetHashCode() & hash_mask;
		}

		private E Convert(LightWeightGSet.LinkedElement e)
		{
			E r = (E)e;
			return r;
		}

		public override E Get(K key)
		{
			//validate key
			if (key == null)
			{
				throw new ArgumentNullException("key == null");
			}
			//find element
			int index = GetIndex(key);
			for (LightWeightGSet.LinkedElement e = entries[index]; e != null; e = e.GetNext())
			{
				if (e.Equals(key))
				{
					return Convert(e);
				}
			}
			//element not found
			return null;
		}

		public override bool Contains(K key)
		{
			return Get(key) != null;
		}

		public override E Put(E element)
		{
			//validate element
			if (element == null)
			{
				throw new ArgumentNullException("Null element is not supported.");
			}
			if (!(element is LightWeightGSet.LinkedElement))
			{
				throw new HadoopIllegalArgumentException("!(element instanceof LinkedElement), element.getClass()="
					 + element.GetType());
			}
			LightWeightGSet.LinkedElement e = (LightWeightGSet.LinkedElement)element;
			//find index
			int index = GetIndex(element);
			//remove if it already exists
			E existing = Remove(index, element);
			//insert the element to the head of the linked list
			modification++;
			size++;
			e.SetNext(entries[index]);
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
		private E Remove(int index, K key)
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
					LightWeightGSet.LinkedElement e = entries[index];
					entries[index] = e.GetNext();
					e.SetNext(null);
					return Convert(e);
				}
				else
				{
					//head != null and key is not equal to head
					//search the element
					LightWeightGSet.LinkedElement prev = entries[index];
					for (LightWeightGSet.LinkedElement curr = prev.GetNext(); curr != null; )
					{
						if (curr.Equals(key))
						{
							//found the element, remove it
							modification++;
							size--;
							prev.SetNext(curr.GetNext());
							curr.SetNext(null);
							return Convert(curr);
						}
						else
						{
							prev = curr;
							curr = curr.GetNext();
						}
					}
					//element not found
					return null;
				}
			}
		}

		public override E Remove(K key)
		{
			//validate key
			if (key == null)
			{
				throw new ArgumentNullException("key == null");
			}
			return Remove(GetIndex(key), key);
		}

		public virtual IEnumerator<E> GetEnumerator()
		{
			return new LightWeightGSet.SetIterator(this);
		}

		public override string ToString()
		{
			StringBuilder b = new StringBuilder(GetType().Name);
			b.Append("(size=").Append(size).Append(string.Format(", %08x", hash_mask)).Append
				(", modification=").Append(modification).Append(", entries.length=").Append(entries
				.Length).Append(")");
			return b.ToString();
		}

		/// <summary>Print detailed information of this object.</summary>
		public virtual void PrintDetails(TextWriter @out)
		{
			@out.Write(this + ", entries = [");
			for (int i = 0; i < entries.Length; i++)
			{
				if (entries[i] != null)
				{
					LightWeightGSet.LinkedElement e = entries[i];
					@out.Write("\n  " + i + ": " + e);
					for (e = e.GetNext(); e != null; e = e.GetNext())
					{
						@out.Write(" -> " + e);
					}
				}
			}
			@out.WriteLine("\n]");
		}

		public class SetIterator : IEnumerator<E>
		{
			/// <summary>The starting modification for fail-fast.</summary>
			private int iterModification = this._enclosing.modification;

			/// <summary>The current index of the entry array.</summary>
			private int index = -1;

			private LightWeightGSet.LinkedElement cur = null;

			private LightWeightGSet.LinkedElement next = this.NextNonemptyEntry();

			private bool trackModification = true;

			/// <summary>Find the next nonempty entry starting at (index + 1).</summary>
			private LightWeightGSet.LinkedElement NextNonemptyEntry()
			{
				for (this.index++; this.index < this._enclosing.entries.Length && this._enclosing
					.entries[this.index] == null; this.index++)
				{
				}
				return this.index < this._enclosing.entries.Length ? this._enclosing.entries[this
					.index] : null;
			}

			private void EnsureNext()
			{
				if (this.trackModification && this._enclosing.modification != this.iterModification)
				{
					throw new ConcurrentModificationException("modification=" + this._enclosing.modification
						 + " != iterModification = " + this.iterModification);
				}
				if (this.next != null)
				{
					return;
				}
				if (this.cur == null)
				{
					return;
				}
				this.next = this.cur.GetNext();
				if (this.next == null)
				{
					this.next = this.NextNonemptyEntry();
				}
			}

			public override bool HasNext()
			{
				this.EnsureNext();
				return this.next != null;
			}

			public override E Next()
			{
				this.EnsureNext();
				if (this.next == null)
				{
					throw new InvalidOperationException("There are no more elements");
				}
				this.cur = this.next;
				this.next = null;
				return this._enclosing.Convert(this.cur);
			}

			public override void Remove()
			{
				this.EnsureNext();
				if (this.cur == null)
				{
					throw new InvalidOperationException("There is no current element " + "to remove");
				}
				this._enclosing._enclosing.Remove((K)this.cur);
				this.iterModification++;
				this.cur = null;
			}

			public virtual void SetTrackModification(bool trackModification)
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
		public static int ComputeCapacity(double percentage, string mapName)
		{
			return ComputeCapacity(Runtime.GetRuntime().MaxMemory(), percentage, mapName);
		}

		[VisibleForTesting]
		internal static int ComputeCapacity(long maxMemory, double percentage, string mapName
			)
		{
			if (percentage > 100.0 || percentage < 0.0)
			{
				throw new HadoopIllegalArgumentException("Percentage " + percentage + " must be greater than or equal to 0 "
					 + " and less than or equal to 100");
			}
			if (maxMemory < 0)
			{
				throw new HadoopIllegalArgumentException("Memory " + maxMemory + " must be greater than or equal to 0"
					);
			}
			if (percentage == 0.0 || maxMemory == 0)
			{
				return 0;
			}
			//VM detection
			//See http://java.sun.com/docs/hotspot/HotSpotFAQ.html#64bit_detection
			string vmBit = Runtime.GetProperty("sun.arch.data.model");
			//Percentage of max memory
			double percentDivisor = 100.0 / percentage;
			double percentMemory = maxMemory / percentDivisor;
			//compute capacity
			int e1 = (int)(Math.Log(percentMemory) / Math.Log(2.0) + 0.5);
			int e2 = e1 - ("32".Equals(vmBit) ? 2 : 3);
			int exponent = e2 < 0 ? 0 : e2 > 30 ? 30 : e2;
			int c = 1 << exponent;
			Log.Info("Computing capacity for map " + mapName);
			Log.Info("VM type       = " + vmBit + "-bit");
			Log.Info(percentage + "% max memory " + StringUtils.TraditionalBinaryPrefix.Long2String
				(maxMemory, "B", 1) + " = " + StringUtils.TraditionalBinaryPrefix.Long2String((long
				)percentMemory, "B", 1));
			Log.Info("capacity      = 2^" + exponent + " = " + c + " entries");
			return c;
		}

		public override void Clear()
		{
			for (int i = 0; i < entries.Length; i++)
			{
				entries[i] = null;
			}
			size = 0;
		}
	}
}
