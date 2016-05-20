using Sharpen;

namespace org.apache.hadoop.util.bloom
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
	public sealed class RetouchedBloomFilter : org.apache.hadoop.util.bloom.BloomFilter
		, org.apache.hadoop.util.bloom.RemoveScheme
	{
		/// <summary>KeyList vector (or ElementList Vector, as defined in the paper) of false positives.
		/// 	</summary>
		internal System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key>[] fpVector;

		/// <summary>KeyList vector of keys recorded in the filter.</summary>
		internal System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key>[] keyVector;

		/// <summary>Ratio vector.</summary>
		internal double[] ratio;

		private java.util.Random rand;

		/// <summary>Default constructor - use with readFields</summary>
		public RetouchedBloomFilter()
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="vectorSize">The vector size of <i>this</i> filter.</param>
		/// <param name="nbHash">The number of hash function to consider.</param>
		/// <param name="hashType">
		/// type of the hashing function (see
		/// <see cref="org.apache.hadoop.util.hash.Hash"/>
		/// ).
		/// </param>
		public RetouchedBloomFilter(int vectorSize, int nbHash, int hashType)
			: base(vectorSize, nbHash, hashType)
		{
			this.rand = null;
			createVector();
		}

		public override void add(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				throw new System.ArgumentNullException("key can not be null");
			}
			int[] h = hash.hash(key);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				bits.set(h[i]);
				keyVector[h[i]].add(key);
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
		public void addFalsePositive(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				throw new System.ArgumentNullException("key can not be null");
			}
			int[] h = hash.hash(key);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				fpVector[h[i]].add(key);
			}
		}

		/// <summary>Adds a collection of false positive information to <i>this</i> retouched Bloom filter.
		/// 	</summary>
		/// <param name="coll">The collection of false positive.</param>
		public void addFalsePositive(System.Collections.Generic.ICollection<org.apache.hadoop.util.bloom.Key
			> coll)
		{
			if (coll == null)
			{
				throw new System.ArgumentNullException("Collection<Key> can not be null");
			}
			foreach (org.apache.hadoop.util.bloom.Key k in coll)
			{
				addFalsePositive(k);
			}
		}

		/// <summary>Adds a list of false positive information to <i>this</i> retouched Bloom filter.
		/// 	</summary>
		/// <param name="keys">The list of false positive.</param>
		public void addFalsePositive(System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key
			> keys)
		{
			if (keys == null)
			{
				throw new System.ArgumentNullException("ArrayList<Key> can not be null");
			}
			foreach (org.apache.hadoop.util.bloom.Key k in keys)
			{
				addFalsePositive(k);
			}
		}

		/// <summary>Adds an array of false positive information to <i>this</i> retouched Bloom filter.
		/// 	</summary>
		/// <param name="keys">The array of false positive.</param>
		public void addFalsePositive(org.apache.hadoop.util.bloom.Key[] keys)
		{
			if (keys == null)
			{
				throw new System.ArgumentNullException("Key[] can not be null");
			}
			for (int i = 0; i < keys.Length; i++)
			{
				addFalsePositive(keys[i]);
			}
		}

		/// <summary>Performs the selective clearing for a given key.</summary>
		/// <param name="k">The false positive key to remove from <i>this</i> retouched Bloom filter.
		/// 	</param>
		/// <param name="scheme">The selective clearing scheme to apply.</param>
		public void selectiveClearing(org.apache.hadoop.util.bloom.Key k, short scheme)
		{
			if (k == null)
			{
				throw new System.ArgumentNullException("Key can not be null");
			}
			if (!membershipTest(k))
			{
				throw new System.ArgumentException("Key is not a member");
			}
			int index = 0;
			int[] h = hash.hash(k);
			switch (scheme)
			{
				case RANDOM:
				{
					index = randomRemove();
					break;
				}

				case MINIMUM_FN:
				{
					index = minimumFnRemove(h);
					break;
				}

				case MAXIMUM_FP:
				{
					index = maximumFpRemove(h);
					break;
				}

				case RATIO:
				{
					index = ratioRemove(h);
					break;
				}

				default:
				{
					throw new java.lang.AssertionError("Undefined selective clearing scheme");
				}
			}
			clearBit(index);
		}

		private int randomRemove()
		{
			if (rand == null)
			{
				rand = new java.util.Random();
			}
			return rand.nextInt(nbHash);
		}

		/// <summary>Chooses the bit position that minimizes the number of false negative generated.
		/// 	</summary>
		/// <param name="h">The different bit positions.</param>
		/// <returns>The position that minimizes the number of false negative generated.</returns>
		private int minimumFnRemove(int[] h)
		{
			int minIndex = int.MaxValue;
			double minValue = double.MaxValue;
			for (int i = 0; i < nbHash; i++)
			{
				double keyWeight = getWeight(keyVector[h[i]]);
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
		private int maximumFpRemove(int[] h)
		{
			int maxIndex = int.MinValue;
			double maxValue = double.MinValue;
			for (int i = 0; i < nbHash; i++)
			{
				double fpWeight = getWeight(fpVector[h[i]]);
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
		private int ratioRemove(int[] h)
		{
			computeRatio();
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
		private void clearBit(int index)
		{
			if (index < 0 || index >= vectorSize)
			{
				throw new System.IndexOutOfRangeException(index);
			}
			System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key> kl = keyVector
				[index];
			System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key> fpl = fpVector
				[index];
			// update key list
			int listSize = kl.Count;
			for (int i = 0; i < listSize && !kl.isEmpty(); i++)
			{
				removeKey(kl[0], keyVector);
			}
			kl.clear();
			keyVector[index].clear();
			//update false positive list
			listSize = fpl.Count;
			for (int i_1 = 0; i_1 < listSize && !fpl.isEmpty(); i_1++)
			{
				removeKey(fpl[0], fpVector);
			}
			fpl.clear();
			fpVector[index].clear();
			//update ratio
			ratio[index] = 0.0;
			//update bit vector
			bits.clear(index);
		}

		/// <summary>Removes a given key from <i>this</i> filer.</summary>
		/// <param name="k">The key to remove.</param>
		/// <param name="vector">The counting vector associated to the key.</param>
		private void removeKey(org.apache.hadoop.util.bloom.Key k, System.Collections.Generic.IList
			<org.apache.hadoop.util.bloom.Key>[] vector)
		{
			if (k == null)
			{
				throw new System.ArgumentNullException("Key can not be null");
			}
			if (vector == null)
			{
				throw new System.ArgumentNullException("ArrayList<Key>[] can not be null");
			}
			int[] h = hash.hash(k);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				vector[h[i]].remove(k);
			}
		}

		/// <summary>Computes the ratio A/FP.</summary>
		private void computeRatio()
		{
			for (int i = 0; i < vectorSize; i++)
			{
				double keyWeight = getWeight(keyVector[i]);
				double fpWeight = getWeight(fpVector[i]);
				if (keyWeight > 0 && fpWeight > 0)
				{
					ratio[i] = keyWeight / fpWeight;
				}
			}
		}

		private double getWeight(System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key
			> keyList)
		{
			double weight = 0.0;
			foreach (org.apache.hadoop.util.bloom.Key k in keyList)
			{
				weight += k.getWeight();
			}
			return weight;
		}

		/// <summary>Creates and initialises the various vectors.</summary>
		private void createVector()
		{
			fpVector = new System.Collections.IList[vectorSize];
			keyVector = new System.Collections.IList[vectorSize];
			ratio = new double[vectorSize];
			for (int i = 0; i < vectorSize; i++)
			{
				fpVector[i] = java.util.Collections.synchronizedList(new System.Collections.Generic.List
					<org.apache.hadoop.util.bloom.Key>());
				keyVector[i] = java.util.Collections.synchronizedList(new System.Collections.Generic.List
					<org.apache.hadoop.util.bloom.Key>());
				ratio[i] = 0.0;
			}
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public override void write(java.io.DataOutput @out)
		{
			base.write(@out);
			for (int i = 0; i < fpVector.Length; i++)
			{
				System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key> list = fpVector
					[i];
				@out.writeInt(list.Count);
				foreach (org.apache.hadoop.util.bloom.Key k in list)
				{
					k.write(@out);
				}
			}
			for (int i_1 = 0; i_1 < keyVector.Length; i_1++)
			{
				System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key> list = keyVector
					[i_1];
				@out.writeInt(list.Count);
				foreach (org.apache.hadoop.util.bloom.Key k in list)
				{
					k.write(@out);
				}
			}
			for (int i_2 = 0; i_2 < ratio.Length; i_2++)
			{
				@out.writeDouble(ratio[i_2]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void readFields(java.io.DataInput @in)
		{
			base.readFields(@in);
			createVector();
			for (int i = 0; i < fpVector.Length; i++)
			{
				System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key> list = fpVector
					[i];
				int size = @in.readInt();
				for (int j = 0; j < size; j++)
				{
					org.apache.hadoop.util.bloom.Key k = new org.apache.hadoop.util.bloom.Key();
					k.readFields(@in);
					list.add(k);
				}
			}
			for (int i_1 = 0; i_1 < keyVector.Length; i_1++)
			{
				System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key> list = keyVector
					[i_1];
				int size = @in.readInt();
				for (int j = 0; j < size; j++)
				{
					org.apache.hadoop.util.bloom.Key k = new org.apache.hadoop.util.bloom.Key();
					k.readFields(@in);
					list.add(k);
				}
			}
			for (int i_2 = 0; i_2 < ratio.Length; i_2++)
			{
				ratio[i_2] = @in.readDouble();
			}
		}
	}
}
