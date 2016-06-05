using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class implements the sort interface using primitive int arrays as
	/// the data structures (that is why this class is called 'BasicType'SorterBase)
	/// </summary>
	internal abstract class BasicTypeSorterBase : BufferSorter
	{
		protected internal OutputBuffer keyValBuffer;

		protected internal int[] startOffsets;

		protected internal int[] keyLengths;

		protected internal int[] valueLengths;

		protected internal int[] pointers;

		protected internal RawComparator comparator;

		protected internal int count;

		private const int BufferedKeyValOverhead = 16;

		private const int InitialArraySize = 5;

		private int maxKeyLength = 0;

		private int maxValLength = 0;

		protected internal Progressable reporter;

		//the buffer used for storing
		//key/values
		//the array used to store the start offsets of
		//keys in keyValBuffer
		//the array used to store the lengths of
		//keys
		//the array used to store the value lengths 
		//the array of startOffsets's indices. This will
		//be sorted at the end to contain a sorted array of
		//indices to offsets
		//the comparator for the map output
		//the number of key/values
		//the overhead of the arrays in memory 
		//12 => 4 for keyoffsets, 4 for keylengths, 4 for valueLengths, and
		//4 for indices into startOffsets array in the
		//pointers array (ignored the partpointers list itself)
		//we maintain the max lengths of the key/val that we encounter.  During 
		//iteration of the sorted results, we will create a DataOutputBuffer to
		//return the keys. The max size of the DataOutputBuffer will be the max
		//keylength that we encounter. Expose this value to model memory more
		//accurately.
		//Reference to the Progressable object for sending KeepAlive
		//Implementation of methods of the SorterBase interface
		//
		public virtual void Configure(JobConf conf)
		{
			comparator = conf.GetOutputKeyComparator();
		}

		public virtual void SetProgressable(Progressable reporter)
		{
			this.reporter = reporter;
		}

		public virtual void AddKeyValue(int recordOffset, int keyLength, int valLength)
		{
			//Add the start offset of the key in the startOffsets array and the
			//length in the keyLengths array.
			if (startOffsets == null || count == startOffsets.Length)
			{
				Grow();
			}
			startOffsets[count] = recordOffset;
			keyLengths[count] = keyLength;
			if (keyLength > maxKeyLength)
			{
				maxKeyLength = keyLength;
			}
			if (valLength > maxValLength)
			{
				maxValLength = valLength;
			}
			valueLengths[count] = valLength;
			pointers[count] = count;
			count++;
		}

		public virtual void SetInputBuffer(OutputBuffer buffer)
		{
			//store a reference to the keyValBuffer that we need to read during sort
			this.keyValBuffer = buffer;
		}

		public virtual long GetMemoryUtilized()
		{
			//the total length of the arrays + the max{Key,Val}Length (this will be the 
			//max size of the DataOutputBuffers during the iteration of the sorted
			//keys).
			if (startOffsets != null)
			{
				return (startOffsets.Length) * BufferedKeyValOverhead + maxKeyLength + maxValLength;
			}
			else
			{
				//nothing from this yet
				return 0;
			}
		}

		public abstract SequenceFile.Sorter.RawKeyValueIterator Sort();

		public virtual void Close()
		{
			//set count to 0; also, we don't reuse the arrays since we want to maintain
			//consistency in the memory model
			count = 0;
			startOffsets = null;
			keyLengths = null;
			valueLengths = null;
			pointers = null;
			maxKeyLength = 0;
			maxValLength = 0;
			//release the large key-value buffer so that the GC, if necessary,
			//can collect it away
			keyValBuffer = null;
		}

		private void Grow()
		{
			int currLength = 0;
			if (startOffsets != null)
			{
				currLength = startOffsets.Length;
			}
			int newLength = (int)(currLength * 1.1) + 1;
			startOffsets = Grow(startOffsets, newLength);
			keyLengths = Grow(keyLengths, newLength);
			valueLengths = Grow(valueLengths, newLength);
			pointers = Grow(pointers, newLength);
		}

		private int[] Grow(int[] old, int newLength)
		{
			int[] result = new int[newLength];
			if (old != null)
			{
				System.Array.Copy(old, 0, result, 0, old.Length);
			}
			return result;
		}
	}

	internal class MRSortResultIterator : SequenceFile.Sorter.RawKeyValueIterator
	{
		private int count;

		private int[] pointers;

		private int[] startOffsets;

		private int[] keyLengths;

		private int[] valLengths;

		private int currStartOffsetIndex;

		private int currIndexInPointers;

		private OutputBuffer keyValBuffer;

		private DataOutputBuffer key = new DataOutputBuffer();

		private MRSortResultIterator.InMemUncompressedBytes value = new MRSortResultIterator.InMemUncompressedBytes
			();

		public MRSortResultIterator(OutputBuffer keyValBuffer, int[] pointers, int[] startOffsets
			, int[] keyLengths, int[] valLengths)
		{
			//BasicTypeSorterBase
			//Implementation of methods of the RawKeyValueIterator interface. These
			//methods must be invoked to iterate over key/vals after sort is done.
			//
			this.count = pointers.Length;
			this.pointers = pointers;
			this.startOffsets = startOffsets;
			this.keyLengths = keyLengths;
			this.valLengths = valLengths;
			this.keyValBuffer = keyValBuffer;
		}

		public virtual Progress GetProgress()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual DataOutputBuffer GetKey()
		{
			int currKeyOffset = startOffsets[currStartOffsetIndex];
			int currKeyLength = keyLengths[currStartOffsetIndex];
			//reuse the same key
			key.Reset();
			key.Write(keyValBuffer.GetData(), currKeyOffset, currKeyLength);
			return key;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual SequenceFile.ValueBytes GetValue()
		{
			//value[i] is stored in the following byte range:
			//startOffsets[i] + keyLengths[i] through valLengths[i]
			value.Reset(keyValBuffer, startOffsets[currStartOffsetIndex] + keyLengths[currStartOffsetIndex
				], valLengths[currStartOffsetIndex]);
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next()
		{
			if (count == currIndexInPointers)
			{
				return false;
			}
			currStartOffsetIndex = pointers[currIndexInPointers];
			currIndexInPointers++;
			return true;
		}

		public virtual void Close()
		{
			return;
		}

		private class InMemUncompressedBytes : SequenceFile.ValueBytes
		{
			private byte[] data;

			internal int start;

			internal int dataSize;

			//An implementation of the ValueBytes interface for the in-memory value
			//buffers. 
			/// <exception cref="System.IO.IOException"/>
			private void Reset(OutputBuffer d, int start, int length)
			{
				data = d.GetData();
				this.start = start;
				dataSize = length;
			}

			public virtual int GetSize()
			{
				return dataSize;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteUncompressedBytes(DataOutputStream outStream)
			{
				outStream.Write(data, start, dataSize);
			}

			/// <exception cref="System.ArgumentException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteCompressedBytes(DataOutputStream outStream)
			{
				throw new ArgumentException("UncompressedBytes cannot be compressed!");
			}
		}
		// InMemUncompressedBytes
	}
}
