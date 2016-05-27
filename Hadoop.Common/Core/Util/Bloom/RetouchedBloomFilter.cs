using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Util.Bloom
{
	/// <summary>Implements a <i>retouched Bloom filter</i>, as defined in the CoNEXT 2006 paper.
	/// 	</summary>
	/// <remarks>
	/// Implements a <i>retouched Bloom filter</i>, as defined in the CoNEXT 2006 paper.
	/// <p>
	/// It allows the removal of selected false positives at the cost of introducing
	/// random false negatives, and with the benefit of eliminating some random false
	/// positives at the same time.
	/// <p>
	/// Originally created by
	/// <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
	/// </remarks>
	/// <seealso cref="Filter">The general behavior of a filter</seealso>
	/// <seealso cref="BloomFilter">A Bloom filter</seealso>
	/// <seealso cref="RemoveScheme">The different selective clearing algorithms</seealso>
	/// <seealso><a href="http://www-rp.lip6.fr/site_npa/site_rp/_publications/740-rbf_cameraready.pdf">Retouched Bloom Filters: Allowing Networked Applications to Trade Off Selected False Positives Against False Negatives</a>
	/// 	</seealso>
	public sealed class RetouchedBloomFilter : BloomFilter, RemoveScheme
	{
		/// <summary>KeyList vector (or ElementList Vector, as defined in the paper) of false positives.
		/// 	</summary>
		internal IList<Key>[] fpVector;

		/// <summary>KeyList vector of keys recorded in the filter.</summary>
		internal IList<Key>[] keyVector;

		/// <summary>Ratio vector.</summary>
		internal double[] ratio;

		private Random rand;

		/// <summary>Default constructor - use with readFields</summary>
		public RetouchedBloomFilter()
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="vectorSize">The vector size of <i>this</i> filter.</param>
		/// <param name="nbHash">The number of hash function to consider.</param>
		/// <param name="hashType">
		/// type of the hashing function (see
		/// <see cref="Org.Apache.Hadoop.Util.Hash.Hash"/>
		/// ).
		/// </param>
		public RetouchedBloomFilter(int vectorSize, int nbHash, int hashType)
			: base(vectorSize, nbHash, hashType)
		{
			this.rand = null;
			CreateVector();
		}

		public override void Add(Key key)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key can not be null");
			}
			int[] h = hash.Hash(key);
			hash.Clear();
			for (int i = 0; i < nbHash; i++)
			{
				bits.Set(h[i]);
				keyVector[h[i]].AddItem(key);
			}
		}

		/// <summary>Adds a false positive information to <i>this</i> retouched Bloom filter.
		/// 	</summary>
		/// <remarks>
		/// Adds a false positive information to <i>this</i> retouched Bloom filter.
		/// <p>
		/// <b>Invariant</b>: if the false positive is <code>null</code>, nothing happens.
		/// </remarks>
		/// <param name="key">The false positive key to add.</param>
		public void AddFalsePositive(Key key)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key can not be null");
			}
			int[] h = hash.Hash(key);
			hash.Clear();
			for (int i = 0; i < nbHash; i++)
			{
				fpVector[h[i]].AddItem(key);
			}
		}

		/// <summary>Adds a collection of false positive information to <i>this</i> retouched Bloom filter.
		/// 	</summary>
		/// <param name="coll">The collection of false positive.</param>
		public void AddFalsePositive(ICollection<Key> coll)
		{
			if (coll == null)
			{
				throw new ArgumentNullException("Collection<Key> can not be null");
			}
			foreach (Key k in coll)
			{
				AddFalsePositive(k);
			}
		}

		/// <summary>Adds a list of false positive information to <i>this</i> retouched Bloom filter.
		/// 	</summary>
		/// <param name="keys">The list of false positive.</param>
		public void AddFalsePositive(IList<Key> keys)
		{
			if (keys == null)
			{
				throw new ArgumentNullException("ArrayList<Key> can not be null");
			}
			foreach (Key k in keys)
			{
				AddFalsePositive(k);
			}
		}

		/// <summary>Adds an array of false positive information to <i>this</i> retouched Bloom filter.
		/// 	</summary>
		/// <param name="keys">The array of false positive.</param>
		public void AddFalsePositive(Key[] keys)
		{
			if (keys == null)
			{
				throw new ArgumentNullException("Key[] can not be null");
			}
			for (int i = 0; i < keys.Length; i++)
			{
				AddFalsePositive(keys[i]);
			}
		}

		/// <summary>Performs the selective clearing for a given key.</summary>
		/// <param name="k">The false positive key to remove from <i>this</i> retouched Bloom filter.
		/// 	</param>
		/// <param name="scheme">The selective clearing scheme to apply.</param>
		public void SelectiveClearing(Key k, short scheme)
		{
			if (k == null)
			{
				throw new ArgumentNullException("Key can not be null");
			}
			if (!MembershipTest(k))
			{
				throw new ArgumentException("Key is not a member");
			}
			int index = 0;
			int[] h = hash.Hash(k);
			switch (scheme)
			{
				case Random:
				{
					index = RandomRemove();
					break;
				}

				case MinimumFn:
				{
					index = MinimumFnRemove(h);
					break;
				}

				case MaximumFp:
				{
					index = MaximumFpRemove(h);
					break;
				}

				case Ratio:
				{
					index = RatioRemove(h);
					break;
				}

				default:
				{
					throw new Exception("Undefined selective clearing scheme");
				}
			}
			ClearBit(index);
		}

		private int RandomRemove()
		{
			if (rand == null)
			{
				rand = new Random();
			}
			return rand.Next(nbHash);
		}

		/// <summary>Chooses the bit position that minimizes the number of false negative generated.
		/// 	</summary>
		/// <param name="h">The different bit positions.</param>
		/// <returns>The position that minimizes the number of false negative generated.</returns>
		private int MinimumFnRemove(int[] h)
		{
			int minIndex = int.MaxValue;
			double minValue = double.MaxValue;
			for (int i = 0; i < nbHash; i++)
			{
				double keyWeight = GetWeight(keyVector[h[i]]);
				if (keyWeight < minValue)
				{
					minIndex = h[i];
					minValue = keyWeight;
				}
			}
			return minIndex;
		}

		/// <summary>Chooses the bit position that maximizes the number of false positive removed.
		/// 	</summary>
		/// <param name="h">The different bit positions.</param>
		/// <returns>The position that maximizes the number of false positive removed.</returns>
		private int MaximumFpRemove(int[] h)
		{
			int maxIndex = int.MinValue;
			double maxValue = double.MinValue;
			for (int i = 0; i < nbHash; i++)
			{
				double fpWeight = GetWeight(fpVector[h[i]]);
				if (fpWeight > maxValue)
				{
					maxValue = fpWeight;
					maxIndex = h[i];
				}
			}
			return maxIndex;
		}

		/// <summary>Chooses the bit position that minimizes the number of false negative generated while maximizing.
		/// 	</summary>
		/// <remarks>
		/// Chooses the bit position that minimizes the number of false negative generated while maximizing.
		/// the number of false positive removed.
		/// </remarks>
		/// <param name="h">The different bit positions.</param>
		/// <returns>The position that minimizes the number of false negative generated while maximizing.
		/// 	</returns>
		private int RatioRemove(int[] h)
		{
			ComputeRatio();
			int minIndex = int.MaxValue;
			double minValue = double.MaxValue;
			for (int i = 0; i < nbHash; i++)
			{
				if (ratio[h[i]] < minValue)
				{
					minValue = ratio[h[i]];
					minIndex = h[i];
				}
			}
			return minIndex;
		}

		/// <summary>Clears a specified bit in the bit vector and keeps up-to-date the KeyList vectors.
		/// 	</summary>
		/// <param name="index">The position of the bit to clear.</param>
		private void ClearBit(int index)
		{
			if (index < 0 || index >= vectorSize)
			{
				throw Sharpen.Extensions.CreateIndexOutOfRangeException(index);
			}
			IList<Key> kl = keyVector[index];
			IList<Key> fpl = fpVector[index];
			// update key list
			int listSize = kl.Count;
			for (int i = 0; i < listSize && !kl.IsEmpty(); i++)
			{
				RemoveKey(kl[0], keyVector);
			}
			kl.Clear();
			keyVector[index].Clear();
			//update false positive list
			listSize = fpl.Count;
			for (int i_1 = 0; i_1 < listSize && !fpl.IsEmpty(); i_1++)
			{
				RemoveKey(fpl[0], fpVector);
			}
			fpl.Clear();
			fpVector[index].Clear();
			//update ratio
			ratio[index] = 0.0;
			//update bit vector
			bits.Clear(index);
		}

		/// <summary>Removes a given key from <i>this</i> filer.</summary>
		/// <param name="k">The key to remove.</param>
		/// <param name="vector">The counting vector associated to the key.</param>
		private void RemoveKey(Key k, IList<Key>[] vector)
		{
			if (k == null)
			{
				throw new ArgumentNullException("Key can not be null");
			}
			if (vector == null)
			{
				throw new ArgumentNullException("ArrayList<Key>[] can not be null");
			}
			int[] h = hash.Hash(k);
			hash.Clear();
			for (int i = 0; i < nbHash; i++)
			{
				vector[h[i]].Remove(k);
			}
		}

		/// <summary>Computes the ratio A/FP.</summary>
		private void ComputeRatio()
		{
			for (int i = 0; i < vectorSize; i++)
			{
				double keyWeight = GetWeight(keyVector[i]);
				double fpWeight = GetWeight(fpVector[i]);
				if (keyWeight > 0 && fpWeight > 0)
				{
					ratio[i] = keyWeight / fpWeight;
				}
			}
		}

		private double GetWeight(IList<Key> keyList)
		{
			double weight = 0.0;
			foreach (Key k in keyList)
			{
				weight += k.GetWeight();
			}
			return weight;
		}

		/// <summary>Creates and initialises the various vectors.</summary>
		private void CreateVector()
		{
			fpVector = new IList[vectorSize];
			keyVector = new IList[vectorSize];
			ratio = new double[vectorSize];
			for (int i = 0; i < vectorSize; i++)
			{
				fpVector[i] = Sharpen.Collections.SynchronizedList(new AList<Key>());
				keyVector[i] = Sharpen.Collections.SynchronizedList(new AList<Key>());
				ratio[i] = 0.0;
			}
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			for (int i = 0; i < fpVector.Length; i++)
			{
				IList<Key> list = fpVector[i];
				@out.WriteInt(list.Count);
				foreach (Key k in list)
				{
					k.Write(@out);
				}
			}
			for (int i_1 = 0; i_1 < keyVector.Length; i_1++)
			{
				IList<Key> list = keyVector[i_1];
				@out.WriteInt(list.Count);
				foreach (Key k in list)
				{
					k.Write(@out);
				}
			}
			for (int i_2 = 0; i_2 < ratio.Length; i_2++)
			{
				@out.WriteDouble(ratio[i_2]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			CreateVector();
			for (int i = 0; i < fpVector.Length; i++)
			{
				IList<Key> list = fpVector[i];
				int size = @in.ReadInt();
				for (int j = 0; j < size; j++)
				{
					Key k = new Key();
					k.ReadFields(@in);
					list.AddItem(k);
				}
			}
			for (int i_1 = 0; i_1 < keyVector.Length; i_1++)
			{
				IList<Key> list = keyVector[i_1];
				int size = @in.ReadInt();
				for (int j = 0; j < size; j++)
				{
					Key k = new Key();
					k.ReadFields(@in);
					list.AddItem(k);
				}
			}
			for (int i_2 = 0; i_2 < ratio.Length; i_2++)
			{
				ratio[i_2] = @in.ReadDouble();
			}
		}
	}
}
