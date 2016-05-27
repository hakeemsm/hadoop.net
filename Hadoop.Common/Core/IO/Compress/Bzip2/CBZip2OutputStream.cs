/*
*  Licensed to the Apache Software Foundation (ASF) under one or more
*  contributor license agreements.  See the NOTICE file distributed with
*  this work for additional information regarding copyright ownership.
*  The ASF licenses this file to You under the Apache License, Version 2.0
*  (the "License"); you may not use this file except in compliance with
*  the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
*/
/*
* This package is based on the work done by Keiron Liddle, Aftex Software
* <keiron@aftexsw.com> to whom the Ant project is very grateful for his
* great code.
*/
using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress.Bzip2
{
	/// <summary>
	/// An output stream that compresses into the BZip2 format (without the file
	/// header chars) into another stream.
	/// </summary>
	/// <remarks>
	/// An output stream that compresses into the BZip2 format (without the file
	/// header chars) into another stream.
	/// <p>
	/// The compression requires large amounts of memory. Thus you should call the
	/// <see cref="Close()">close()</see>
	/// method as soon as possible, to force
	/// <tt>CBZip2OutputStream</tt> to release the allocated memory.
	/// </p>
	/// <p>
	/// You can shrink the amount of allocated memory and maybe raise the compression
	/// speed by choosing a lower blocksize, which in turn may cause a lower
	/// compression ratio. You can avoid unnecessary memory allocation by avoiding
	/// using a blocksize which is bigger than the size of the input.
	/// </p>
	/// <p>
	/// You can compute the memory usage for compressing by the following formula:
	/// </p>
	/// <pre>
	/// &lt;code&gt;400k + (9 * blocksize)&lt;/code&gt;.
	/// </pre>
	/// <p>
	/// To get the memory required for decompression by
	/// <see cref="CBZip2InputStream">CBZip2InputStream</see>
	/// use
	/// </p>
	/// <pre>
	/// &lt;code&gt;65k + (5 * blocksize)&lt;/code&gt;.
	/// </pre>
	/// <table width="100%" border="1">
	/// <colgroup> <col width="33%" /> <col width="33%" /> <col width="33%" />
	/// </colgroup>
	/// <tr>
	/// <th colspan="3">Memory usage by blocksize</th>
	/// </tr>
	/// <tr>
	/// <th align="right">Blocksize</th> <th align="right">Compression<br />
	/// memory usage</th> <th align="right">Decompression<br />
	/// memory usage</th>
	/// </tr>
	/// <tr>
	/// <td align="right">100k</td>
	/// <td align="right">1300k</td>
	/// <td align="right">565k</td>
	/// </tr>
	/// <tr>
	/// <td align="right">200k</td>
	/// <td align="right">2200k</td>
	/// <td align="right">1065k</td>
	/// </tr>
	/// <tr>
	/// <td align="right">300k</td>
	/// <td align="right">3100k</td>
	/// <td align="right">1565k</td>
	/// </tr>
	/// <tr>
	/// <td align="right">400k</td>
	/// <td align="right">4000k</td>
	/// <td align="right">2065k</td>
	/// </tr>
	/// <tr>
	/// <td align="right">500k</td>
	/// <td align="right">4900k</td>
	/// <td align="right">2565k</td>
	/// </tr>
	/// <tr>
	/// <td align="right">600k</td>
	/// <td align="right">5800k</td>
	/// <td align="right">3065k</td>
	/// </tr>
	/// <tr>
	/// <td align="right">700k</td>
	/// <td align="right">6700k</td>
	/// <td align="right">3565k</td>
	/// </tr>
	/// <tr>
	/// <td align="right">800k</td>
	/// <td align="right">7600k</td>
	/// <td align="right">4065k</td>
	/// </tr>
	/// <tr>
	/// <td align="right">900k</td>
	/// <td align="right">8500k</td>
	/// <td align="right">4565k</td>
	/// </tr>
	/// </table>
	/// <p>
	/// For decompression <tt>CBZip2InputStream</tt> allocates less memory if the
	/// bzipped input is smaller than one block.
	/// </p>
	/// <p>
	/// Instances of this class are not threadsafe.
	/// </p>
	/// <p>
	/// TODO: Update to BZip2 1.0.1
	/// </p>
	/// </remarks>
	public class CBZip2OutputStream : OutputStream, BZip2Constants
	{
		/// <summary>The minimum supported blocksize <tt> == 1</tt>.</summary>
		public const int MinBlocksize = 1;

		/// <summary>The maximum supported blocksize <tt> == 9</tt>.</summary>
		public const int MaxBlocksize = 9;

		/// <summary>This constant is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This constant is accessible by subclasses for historical purposes. If you
		/// don't know what it means then you don't need it.
		/// </remarks>
		protected internal const int Setmask = (1 << 21);

		/// <summary>This constant is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This constant is accessible by subclasses for historical purposes. If you
		/// don't know what it means then you don't need it.
		/// </remarks>
		protected internal const int Clearmask = (~Setmask);

		/// <summary>This constant is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This constant is accessible by subclasses for historical purposes. If you
		/// don't know what it means then you don't need it.
		/// </remarks>
		protected internal const int GreaterIcost = 15;

		/// <summary>This constant is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This constant is accessible by subclasses for historical purposes. If you
		/// don't know what it means then you don't need it.
		/// </remarks>
		protected internal const int LesserIcost = 0;

		/// <summary>This constant is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This constant is accessible by subclasses for historical purposes. If you
		/// don't know what it means then you don't need it.
		/// </remarks>
		protected internal const int SmallThresh = 20;

		/// <summary>This constant is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This constant is accessible by subclasses for historical purposes. If you
		/// don't know what it means then you don't need it.
		/// </remarks>
		protected internal const int DepthThresh = 10;

		/// <summary>This constant is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This constant is accessible by subclasses for historical purposes. If you
		/// don't know what it means then you don't need it.
		/// </remarks>
		protected internal const int WorkFactor = 30;

		/// <summary>This constant is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This constant is accessible by subclasses for historical purposes. If you
		/// don't know what it means then you don't need it.
		/// <p>
		/// If you are ever unlucky/improbable enough to get a stack overflow whilst
		/// sorting, increase the following constant and try again. In practice I
		/// have never seen the stack go above 27 elems, so the following limit seems
		/// very generous.
		/// </p>
		/// </remarks>
		protected internal const int QsortStackSize = 1000;

		/// <summary>Knuth's increments seem to work better than Incerpi-Sedgewick here.</summary>
		/// <remarks>
		/// Knuth's increments seem to work better than Incerpi-Sedgewick here.
		/// Possibly because the number of elems to sort is usually small, typically
		/// &lt;= 20.
		/// </remarks>
		private static readonly int[] Incs = new int[] { 1, 4, 13, 40, 121, 364, 1093, 3280
			, 9841, 29524, 88573, 265720, 797161, 2391484 };

		/// <summary>This method is accessible by subclasses for historical purposes.</summary>
		/// <remarks>
		/// This method is accessible by subclasses for historical purposes. If you
		/// don't know what it does then you don't need it.
		/// </remarks>
		protected internal static void HbMakeCodeLengths(char[] len, int[] freq, int alphaSize
			, int maxLen)
		{
			/*
			* Nodes and heap entries run from 1. Entry 0 for both the heap and
			* nodes is a sentinel.
			*/
			int[] heap = new int[MaxAlphaSize * 2];
			int[] weight = new int[MaxAlphaSize * 2];
			int[] parent = new int[MaxAlphaSize * 2];
			for (int i = alphaSize; --i >= 0; )
			{
				weight[i + 1] = (freq[i] == 0 ? 1 : freq[i]) << 8;
			}
			for (bool tooLong = true; tooLong; )
			{
				tooLong = false;
				int nNodes = alphaSize;
				int nHeap = 0;
				heap[0] = 0;
				weight[0] = 0;
				parent[0] = -2;
				for (int i_1 = 1; i_1 <= alphaSize; i_1++)
				{
					parent[i_1] = -1;
					nHeap++;
					heap[nHeap] = i_1;
					int zz = nHeap;
					int tmp = heap[zz];
					while (weight[tmp] < weight[heap[zz >> 1]])
					{
						heap[zz] = heap[zz >> 1];
						zz >>= 1;
					}
					heap[zz] = tmp;
				}
				// assert (nHeap < (MAX_ALPHA_SIZE + 2)) : nHeap;
				while (nHeap > 1)
				{
					int n1 = heap[1];
					heap[1] = heap[nHeap];
					nHeap--;
					int yy = 0;
					int zz = 1;
					int tmp = heap[1];
					while (true)
					{
						yy = zz << 1;
						if (yy > nHeap)
						{
							break;
						}
						if ((yy < nHeap) && (weight[heap[yy + 1]] < weight[heap[yy]]))
						{
							yy++;
						}
						if (weight[tmp] < weight[heap[yy]])
						{
							break;
						}
						heap[zz] = heap[yy];
						zz = yy;
					}
					heap[zz] = tmp;
					int n2 = heap[1];
					heap[1] = heap[nHeap];
					nHeap--;
					yy = 0;
					zz = 1;
					tmp = heap[1];
					while (true)
					{
						yy = zz << 1;
						if (yy > nHeap)
						{
							break;
						}
						if ((yy < nHeap) && (weight[heap[yy + 1]] < weight[heap[yy]]))
						{
							yy++;
						}
						if (weight[tmp] < weight[heap[yy]])
						{
							break;
						}
						heap[zz] = heap[yy];
						zz = yy;
					}
					heap[zz] = tmp;
					nNodes++;
					parent[n1] = parent[n2] = nNodes;
					int weight_n1 = weight[n1];
					int weight_n2 = weight[n2];
					weight[nNodes] = (((weight_n1 & unchecked((int)(0xffffff00))) + (weight_n2 & unchecked(
						(int)(0xffffff00)))) | (1 + (((weight_n1 & unchecked((int)(0x000000ff))) > (weight_n2
						 & unchecked((int)(0x000000ff)))) ? (weight_n1 & unchecked((int)(0x000000ff))) : 
						(weight_n2 & unchecked((int)(0x000000ff))))));
					parent[nNodes] = -1;
					nHeap++;
					heap[nHeap] = nNodes;
					tmp = 0;
					zz = nHeap;
					tmp = heap[zz];
					int weight_tmp = weight[tmp];
					while (weight_tmp < weight[heap[zz >> 1]])
					{
						heap[zz] = heap[zz >> 1];
						zz >>= 1;
					}
					heap[zz] = tmp;
				}
				// assert (nNodes < (MAX_ALPHA_SIZE * 2)) : nNodes;
				for (int i_2 = 1; i_2 <= alphaSize; i_2++)
				{
					int j = 0;
					int k = i_2;
					for (int parent_k; (parent_k = parent[k]) >= 0; )
					{
						k = parent_k;
						j++;
					}
					len[i_2 - 1] = (char)j;
					if (j > maxLen)
					{
						tooLong = true;
					}
				}
				if (tooLong)
				{
					for (int i_3 = 1; i_3 < alphaSize; i_3++)
					{
						int j = weight[i_3] >> 8;
						j = 1 + (j >> 1);
						weight[i_3] = j << 8;
					}
				}
			}
		}

		private static void HbMakeCodeLengths(byte[] len, int[] freq, CBZip2OutputStream.Data
			 dat, int alphaSize, int maxLen)
		{
			/*
			* Nodes and heap entries run from 1. Entry 0 for both the heap and
			* nodes is a sentinel.
			*/
			int[] heap = dat.heap;
			int[] weight = dat.weight;
			int[] parent = dat.parent;
			for (int i = alphaSize; --i >= 0; )
			{
				weight[i + 1] = (freq[i] == 0 ? 1 : freq[i]) << 8;
			}
			for (bool tooLong = true; tooLong; )
			{
				tooLong = false;
				int nNodes = alphaSize;
				int nHeap = 0;
				heap[0] = 0;
				weight[0] = 0;
				parent[0] = -2;
				for (int i_1 = 1; i_1 <= alphaSize; i_1++)
				{
					parent[i_1] = -1;
					nHeap++;
					heap[nHeap] = i_1;
					int zz = nHeap;
					int tmp = heap[zz];
					while (weight[tmp] < weight[heap[zz >> 1]])
					{
						heap[zz] = heap[zz >> 1];
						zz >>= 1;
					}
					heap[zz] = tmp;
				}
				while (nHeap > 1)
				{
					int n1 = heap[1];
					heap[1] = heap[nHeap];
					nHeap--;
					int yy = 0;
					int zz = 1;
					int tmp = heap[1];
					while (true)
					{
						yy = zz << 1;
						if (yy > nHeap)
						{
							break;
						}
						if ((yy < nHeap) && (weight[heap[yy + 1]] < weight[heap[yy]]))
						{
							yy++;
						}
						if (weight[tmp] < weight[heap[yy]])
						{
							break;
						}
						heap[zz] = heap[yy];
						zz = yy;
					}
					heap[zz] = tmp;
					int n2 = heap[1];
					heap[1] = heap[nHeap];
					nHeap--;
					yy = 0;
					zz = 1;
					tmp = heap[1];
					while (true)
					{
						yy = zz << 1;
						if (yy > nHeap)
						{
							break;
						}
						if ((yy < nHeap) && (weight[heap[yy + 1]] < weight[heap[yy]]))
						{
							yy++;
						}
						if (weight[tmp] < weight[heap[yy]])
						{
							break;
						}
						heap[zz] = heap[yy];
						zz = yy;
					}
					heap[zz] = tmp;
					nNodes++;
					parent[n1] = parent[n2] = nNodes;
					int weight_n1 = weight[n1];
					int weight_n2 = weight[n2];
					weight[nNodes] = ((weight_n1 & unchecked((int)(0xffffff00))) + (weight_n2 & unchecked(
						(int)(0xffffff00)))) | (1 + (((weight_n1 & unchecked((int)(0x000000ff))) > (weight_n2
						 & unchecked((int)(0x000000ff)))) ? (weight_n1 & unchecked((int)(0x000000ff))) : 
						(weight_n2 & unchecked((int)(0x000000ff)))));
					parent[nNodes] = -1;
					nHeap++;
					heap[nHeap] = nNodes;
					tmp = 0;
					zz = nHeap;
					tmp = heap[zz];
					int weight_tmp = weight[tmp];
					while (weight_tmp < weight[heap[zz >> 1]])
					{
						heap[zz] = heap[zz >> 1];
						zz >>= 1;
					}
					heap[zz] = tmp;
				}
				for (int i_2 = 1; i_2 <= alphaSize; i_2++)
				{
					int j = 0;
					int k = i_2;
					for (int parent_k; (parent_k = parent[k]) >= 0; )
					{
						k = parent_k;
						j++;
					}
					len[i_2 - 1] = unchecked((byte)j);
					if (j > maxLen)
					{
						tooLong = true;
					}
				}
				if (tooLong)
				{
					for (int i_3 = 1; i_3 < alphaSize; i_3++)
					{
						int j = weight[i_3] >> 8;
						j = 1 + (j >> 1);
						weight[i_3] = j << 8;
					}
				}
			}
		}

		/// <summary>Index of the last char in the block, so the block size == last + 1.</summary>
		private int last;

		/// <summary>Index in fmap[] of original string after sorting.</summary>
		private int origPtr;

		/// <summary>Always: in the range 0 ..</summary>
		/// <remarks>
		/// Always: in the range 0 .. 9. The current block size is 100000 * this
		/// number.
		/// </remarks>
		private readonly int blockSize100k;

		private bool blockRandomised;

		private int bsBuff;

		private int bsLive;

		private readonly CRC crc = new CRC();

		private int nInUse;

		private int nMTF;

		private int workDone;

		private int workLimit;

		private bool firstAttempt;

		private int currentChar = -1;

		private int runLength = 0;

		private int blockCRC;

		private int combinedCRC;

		private int allowableBlockSize;

		/// <summary>All memory intensive stuff.</summary>
		private CBZip2OutputStream.Data data;

		private OutputStream @out;

		/*
		* Used when sorting. If too many long comparisons happen, we stop sorting,
		* randomise the block slightly, and try again.
		*/
		/// <summary>Chooses a blocksize based on the given length of the data to compress.</summary>
		/// <returns>
		/// The blocksize, between
		/// <see cref="MinBlocksize"/>
		/// and
		/// <see cref="MaxBlocksize"/>
		/// both inclusive. For a negative
		/// <tt>inputLength</tt> this method returns <tt>MAX_BLOCKSIZE</tt>
		/// always.
		/// </returns>
		/// <param name="inputLength">
		/// The length of the data which will be compressed by
		/// <tt>CBZip2OutputStream</tt>.
		/// </param>
		public static int ChooseBlockSize(long inputLength)
		{
			return (inputLength > 0) ? (int)Math.Min((inputLength / 132000) + 1, 9) : MaxBlocksize;
		}

		/// <summary>Constructs a new <tt>CBZip2OutputStream</tt> with a blocksize of 900k.</summary>
		/// <remarks>
		/// Constructs a new <tt>CBZip2OutputStream</tt> with a blocksize of 900k.
		/// <p>
		/// <b>Attention: </b>The caller is resonsible to write the two BZip2 magic
		/// bytes <tt>"BZ"</tt> to the specified stream prior to calling this
		/// constructor.
		/// </p>
		/// </remarks>
		/// <param name="out">
		/// 
		/// the destination stream.
		/// </param>
		/// <exception cref="System.IO.IOException">if an I/O error occurs in the specified stream.
		/// 	</exception>
		/// <exception cref="System.ArgumentNullException">if <code>out == null</code>.</exception>
		public CBZip2OutputStream(OutputStream @out)
			: this(@out, MaxBlocksize)
		{
		}

		/// <summary>Constructs a new <tt>CBZip2OutputStream</tt> with specified blocksize.</summary>
		/// <remarks>
		/// Constructs a new <tt>CBZip2OutputStream</tt> with specified blocksize.
		/// <p>
		/// <b>Attention: </b>The caller is resonsible to write the two BZip2 magic
		/// bytes <tt>"BZ"</tt> to the specified stream prior to calling this
		/// constructor.
		/// </p>
		/// </remarks>
		/// <param name="out">the destination stream.</param>
		/// <param name="blockSize">the blockSize as 100k units.</param>
		/// <exception cref="System.IO.IOException">if an I/O error occurs in the specified stream.
		/// 	</exception>
		/// <exception cref="System.ArgumentException">if <code>(blockSize &lt; 1) || (blockSize &gt; 9)</code>.
		/// 	</exception>
		/// <exception cref="System.ArgumentNullException">if <code>out == null</code>.</exception>
		/// <seealso cref="MinBlocksize"/>
		/// <seealso cref="MaxBlocksize"/>
		public CBZip2OutputStream(OutputStream @out, int blockSize)
			: base()
		{
			if (blockSize < 1)
			{
				throw new ArgumentException("blockSize(" + blockSize + ") < 1");
			}
			if (blockSize > 9)
			{
				throw new ArgumentException("blockSize(" + blockSize + ") > 9");
			}
			this.blockSize100k = blockSize;
			this.@out = @out;
			Init();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			if (this.@out != null)
			{
				Write0(b);
			}
			else
			{
				throw new IOException("closed");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteRun()
		{
			int lastShadow = this.last;
			if (lastShadow < this.allowableBlockSize)
			{
				int currentCharShadow = this.currentChar;
				CBZip2OutputStream.Data dataShadow = this.data;
				dataShadow.inUse[currentCharShadow] = true;
				byte ch = unchecked((byte)currentCharShadow);
				int runLengthShadow = this.runLength;
				this.crc.UpdateCRC(currentCharShadow, runLengthShadow);
				switch (runLengthShadow)
				{
					case 1:
					{
						dataShadow.block[lastShadow + 2] = ch;
						this.last = lastShadow + 1;
						break;
					}

					case 2:
					{
						dataShadow.block[lastShadow + 2] = ch;
						dataShadow.block[lastShadow + 3] = ch;
						this.last = lastShadow + 2;
						break;
					}

					case 3:
					{
						byte[] block = dataShadow.block;
						block[lastShadow + 2] = ch;
						block[lastShadow + 3] = ch;
						block[lastShadow + 4] = ch;
						this.last = lastShadow + 3;
						break;
					}

					default:
					{
						runLengthShadow -= 4;
						dataShadow.inUse[runLengthShadow] = true;
						byte[] block = dataShadow.block;
						block[lastShadow + 2] = ch;
						block[lastShadow + 3] = ch;
						block[lastShadow + 4] = ch;
						block[lastShadow + 5] = ch;
						block[lastShadow + 6] = unchecked((byte)runLengthShadow);
						this.last = lastShadow + 5;
						break;
					}
				}
			}
			else
			{
				EndBlock();
				InitBlock();
				WriteRun();
			}
		}

		~CBZip2OutputStream()
		{
			Finish();
			base.Finalize();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Finish()
		{
			if (@out != null)
			{
				try
				{
					if (this.runLength > 0)
					{
						WriteRun();
					}
					this.currentChar = -1;
					EndBlock();
					EndCompression();
				}
				finally
				{
					this.@out = null;
					this.data = null;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (@out != null)
			{
				OutputStream outShadow = this.@out;
				try
				{
					Finish();
					outShadow.Close();
					outShadow = null;
				}
				finally
				{
					IOUtils.CloseStream(outShadow);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			OutputStream outShadow = this.@out;
			if (outShadow != null)
			{
				outShadow.Flush();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void Init()
		{
			// write magic: done by caller who created this stream
			// this.out.write('B');
			// this.out.write('Z');
			this.data = new CBZip2OutputStream.Data(this.blockSize100k);
			/*
			* Write `magic' bytes h indicating file-format == huffmanised, followed
			* by a digit indicating blockSize100k.
			*/
			BsPutUByte('h');
			BsPutUByte('0' + this.blockSize100k);
			this.combinedCRC = 0;
			InitBlock();
		}

		private void InitBlock()
		{
			// blockNo++;
			this.crc.InitialiseCRC();
			this.last = -1;
			// ch = 0;
			bool[] inUse = this.data.inUse;
			for (int i = 256; --i >= 0; )
			{
				inUse[i] = false;
			}
			/* 20 is just a paranoia constant */
			this.allowableBlockSize = (this.blockSize100k * BZip2Constants.baseBlockSize) - 20;
		}

		/// <exception cref="System.IO.IOException"/>
		private void EndBlock()
		{
			this.blockCRC = this.crc.GetFinalCRC();
			this.combinedCRC = (this.combinedCRC << 1) | ((int)(((uint)this.combinedCRC) >> 31
				));
			this.combinedCRC ^= this.blockCRC;
			// empty block at end of file
			if (this.last == -1)
			{
				return;
			}
			/* sort the block and establish posn of original string */
			BlockSort();
			/*
			* A 6-byte block header, the value chosen arbitrarily as 0x314159265359
			* :-). A 32 bit value does not really give a strong enough guarantee
			* that the value will not appear by chance in the compressed
			* datastream. Worst-case probability of this event, for a 900k block,
			* is about 2.0e-3 for 32 bits, 1.0e-5 for 40 bits and 4.0e-8 for 48
			* bits. For a compressed file of size 100Gb -- about 100000 blocks --
			* only a 48-bit marker will do. NB: normal compression/ decompression
			* donot rely on these statistical properties. They are only important
			* when trying to recover blocks from damaged files.
			*/
			BsPutUByte(unchecked((int)(0x31)));
			BsPutUByte(unchecked((int)(0x41)));
			BsPutUByte(unchecked((int)(0x59)));
			BsPutUByte(unchecked((int)(0x26)));
			BsPutUByte(unchecked((int)(0x53)));
			BsPutUByte(unchecked((int)(0x59)));
			/* Now the block's CRC, so it is in a known place. */
			BsPutInt(this.blockCRC);
			/* Now a single bit indicating randomisation. */
			if (this.blockRandomised)
			{
				BsW(1, 1);
			}
			else
			{
				BsW(1, 0);
			}
			/* Finally, block's contents proper. */
			MoveToFrontCodeAndSend();
		}

		/// <exception cref="System.IO.IOException"/>
		private void EndCompression()
		{
			/*
			* Now another magic 48-bit number, 0x177245385090, to indicate the end
			* of the last block. (sqrt(pi), if you want to know. I did want to use
			* e, but it contains too much repetition -- 27 18 28 18 28 46 -- for me
			* to feel statistically comfortable. Call me paranoid.)
			*/
			BsPutUByte(unchecked((int)(0x17)));
			BsPutUByte(unchecked((int)(0x72)));
			BsPutUByte(unchecked((int)(0x45)));
			BsPutUByte(unchecked((int)(0x38)));
			BsPutUByte(unchecked((int)(0x50)));
			BsPutUByte(unchecked((int)(0x90)));
			BsPutInt(this.combinedCRC);
			BsFinishedWithStream();
		}

		/// <summary>Returns the blocksize parameter specified at construction time.</summary>
		public int GetBlockSize()
		{
			return this.blockSize100k;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] buf, int offs, int len)
		{
			if (offs < 0)
			{
				throw new IndexOutOfRangeException("offs(" + offs + ") < 0.");
			}
			if (len < 0)
			{
				throw new IndexOutOfRangeException("len(" + len + ") < 0.");
			}
			if (offs + len > buf.Length)
			{
				throw new IndexOutOfRangeException("offs(" + offs + ") + len(" + len + ") > buf.length("
					 + buf.Length + ").");
			}
			if (this.@out == null)
			{
				throw new IOException("stream closed");
			}
			for (int hi = offs + len; offs < hi; )
			{
				Write0(buf[offs++]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void Write0(int b)
		{
			if (this.currentChar != -1)
			{
				b &= unchecked((int)(0xff));
				if (this.currentChar == b)
				{
					if (++this.runLength > 254)
					{
						WriteRun();
						this.currentChar = -1;
						this.runLength = 0;
					}
				}
				else
				{
					// else nothing to do
					WriteRun();
					this.runLength = 1;
					this.currentChar = b;
				}
			}
			else
			{
				this.currentChar = b & unchecked((int)(0xff));
				this.runLength++;
			}
		}

		private static void HbAssignCodes(int[] code, byte[] length, int minLen, int maxLen
			, int alphaSize)
		{
			int vec = 0;
			for (int n = minLen; n <= maxLen; n++)
			{
				for (int i = 0; i < alphaSize; i++)
				{
					if ((length[i] & unchecked((int)(0xff))) == n)
					{
						code[i] = vec;
						vec++;
					}
				}
				vec <<= 1;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void BsFinishedWithStream()
		{
			while (this.bsLive > 0)
			{
				int ch = this.bsBuff >> 24;
				this.@out.Write(ch);
				// write 8-bit
				this.bsBuff <<= 8;
				this.bsLive -= 8;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void BsW(int n, int v)
		{
			OutputStream outShadow = this.@out;
			int bsLiveShadow = this.bsLive;
			int bsBuffShadow = this.bsBuff;
			while (bsLiveShadow >= 8)
			{
				outShadow.Write(bsBuffShadow >> 24);
				// write 8-bit
				bsBuffShadow <<= 8;
				bsLiveShadow -= 8;
			}
			this.bsBuff = bsBuffShadow | (v << (32 - bsLiveShadow - n));
			this.bsLive = bsLiveShadow + n;
		}

		/// <exception cref="System.IO.IOException"/>
		private void BsPutUByte(int c)
		{
			BsW(8, c);
		}

		/// <exception cref="System.IO.IOException"/>
		private void BsPutInt(int u)
		{
			BsW(8, (u >> 24) & unchecked((int)(0xff)));
			BsW(8, (u >> 16) & unchecked((int)(0xff)));
			BsW(8, (u >> 8) & unchecked((int)(0xff)));
			BsW(8, u & unchecked((int)(0xff)));
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendMTFValues()
		{
			byte[][] len = this.data.sendMTFValues_len;
			int alphaSize = this.nInUse + 2;
			for (int t = NGroups; --t >= 0; )
			{
				byte[] len_t = len[t];
				for (int v = alphaSize; --v >= 0; )
				{
					len_t[v] = GreaterIcost;
				}
			}
			/* Decide how many coding tables to use */
			// assert (this.nMTF > 0) : this.nMTF;
			int nGroups = (this.nMTF < 200) ? 2 : (this.nMTF < 600) ? 3 : (this.nMTF < 1200) ? 
				4 : (this.nMTF < 2400) ? 5 : 6;
			/* Generate an initial set of coding tables */
			SendMTFValues0(nGroups, alphaSize);
			/*
			* Iterate up to N_ITERS times to improve the tables.
			*/
			int nSelectors = SendMTFValues1(nGroups, alphaSize);
			/* Compute MTF values for the selectors. */
			SendMTFValues2(nGroups, nSelectors);
			/* Assign actual codes for the tables. */
			SendMTFValues3(nGroups, alphaSize);
			/* Transmit the mapping table. */
			SendMTFValues4();
			/* Now the selectors. */
			SendMTFValues5(nGroups, nSelectors);
			/* Now the coding tables. */
			SendMTFValues6(nGroups, alphaSize);
			/* And finally, the block data proper */
			SendMTFValues7(nSelectors);
		}

		private void SendMTFValues0(int nGroups, int alphaSize)
		{
			byte[][] len = this.data.sendMTFValues_len;
			int[] mtfFreq = this.data.mtfFreq;
			int remF = this.nMTF;
			int gs = 0;
			for (int nPart = nGroups; nPart > 0; nPart--)
			{
				int tFreq = remF / nPart;
				int ge = gs - 1;
				int aFreq = 0;
				for (int a = alphaSize - 1; (aFreq < tFreq) && (ge < a); )
				{
					aFreq += mtfFreq[++ge];
				}
				if ((ge > gs) && (nPart != nGroups) && (nPart != 1) && (((nGroups - nPart) & 1) !=
					 0))
				{
					aFreq -= mtfFreq[ge--];
				}
				byte[] len_np = len[nPart - 1];
				for (int v = alphaSize; --v >= 0; )
				{
					if ((v >= gs) && (v <= ge))
					{
						len_np[v] = LesserIcost;
					}
					else
					{
						len_np[v] = GreaterIcost;
					}
				}
				gs = ge + 1;
				remF -= aFreq;
			}
		}

		private int SendMTFValues1(int nGroups, int alphaSize)
		{
			CBZip2OutputStream.Data dataShadow = this.data;
			int[][] rfreq = dataShadow.sendMTFValues_rfreq;
			int[] fave = dataShadow.sendMTFValues_fave;
			short[] cost = dataShadow.sendMTFValues_cost;
			char[] sfmap = dataShadow.sfmap;
			byte[] selector = dataShadow.selector;
			byte[][] len = dataShadow.sendMTFValues_len;
			byte[] len_0 = len[0];
			byte[] len_1 = len[1];
			byte[] len_2 = len[2];
			byte[] len_3 = len[3];
			byte[] len_4 = len[4];
			byte[] len_5 = len[5];
			int nMTFShadow = this.nMTF;
			int nSelectors = 0;
			for (int iter = 0; iter < NIters; iter++)
			{
				for (int t = nGroups; --t >= 0; )
				{
					fave[t] = 0;
					int[] rfreqt = rfreq[t];
					for (int i = alphaSize; --i >= 0; )
					{
						rfreqt[i] = 0;
					}
				}
				nSelectors = 0;
				for (int gs = 0; gs < this.nMTF; )
				{
					/* Set group start & end marks. */
					/*
					* Calculate the cost of this group as coded by each of the
					* coding tables.
					*/
					int ge = Math.Min(gs + GSize - 1, nMTFShadow - 1);
					if (nGroups == NGroups)
					{
						// unrolled version of the else-block
						short cost0 = 0;
						short cost1 = 0;
						short cost2 = 0;
						short cost3 = 0;
						short cost4 = 0;
						short cost5 = 0;
						for (int i = gs; i <= ge; i++)
						{
							int icv = sfmap[i];
							cost0 += len_0[icv] & unchecked((int)(0xff));
							cost1 += len_1[icv] & unchecked((int)(0xff));
							cost2 += len_2[icv] & unchecked((int)(0xff));
							cost3 += len_3[icv] & unchecked((int)(0xff));
							cost4 += len_4[icv] & unchecked((int)(0xff));
							cost5 += len_5[icv] & unchecked((int)(0xff));
						}
						cost[0] = cost0;
						cost[1] = cost1;
						cost[2] = cost2;
						cost[3] = cost3;
						cost[4] = cost4;
						cost[5] = cost5;
					}
					else
					{
						for (int t_1 = nGroups; --t_1 >= 0; )
						{
							cost[t_1] = 0;
						}
						for (int i = gs; i <= ge; i++)
						{
							int icv = sfmap[i];
							for (int t_2 = nGroups; --t_2 >= 0; )
							{
								cost[t_2] += len[t_2][icv] & unchecked((int)(0xff));
							}
						}
					}
					/*
					* Find the coding table which is best for this group, and
					* record its identity in the selector table.
					*/
					int bt = -1;
					for (int t_3 = nGroups; --t_3 >= 0; )
					{
						int cost_t = cost[t_3];
						if (cost_t < bc)
						{
							bc = cost_t;
							bt = t_3;
						}
					}
					fave[bt]++;
					selector[nSelectors] = unchecked((byte)bt);
					nSelectors++;
					/*
					* Increment the symbol frequencies for the selected table.
					*/
					int[] rfreq_bt = rfreq[bt];
					for (int i_1 = gs; i_1 <= ge; i_1++)
					{
						rfreq_bt[sfmap[i_1]]++;
					}
					gs = ge + 1;
				}
				/*
				* Recompute the tables based on the accumulated frequencies.
				*/
				for (int t_4 = 0; t_4 < nGroups; t_4++)
				{
					HbMakeCodeLengths(len[t_4], rfreq[t_4], this.data, alphaSize, 20);
				}
			}
			return nSelectors;
		}

		private void SendMTFValues2(int nGroups, int nSelectors)
		{
			// assert (nGroups < 8) : nGroups;
			CBZip2OutputStream.Data dataShadow = this.data;
			byte[] pos = dataShadow.sendMTFValues2_pos;
			for (int i = nGroups; --i >= 0; )
			{
				pos[i] = unchecked((byte)i);
			}
			for (int i_1 = 0; i_1 < nSelectors; i_1++)
			{
				byte ll_i = dataShadow.selector[i_1];
				byte tmp = pos[0];
				int j = 0;
				while (ll_i != tmp)
				{
					j++;
					byte tmp2 = tmp;
					tmp = pos[j];
					pos[j] = tmp2;
				}
				pos[0] = tmp;
				dataShadow.selectorMtf[i_1] = unchecked((byte)j);
			}
		}

		private void SendMTFValues3(int nGroups, int alphaSize)
		{
			int[][] code = this.data.sendMTFValues_code;
			byte[][] len = this.data.sendMTFValues_len;
			for (int t = 0; t < nGroups; t++)
			{
				int minLen = 32;
				int maxLen = 0;
				byte[] len_t = len[t];
				for (int i = alphaSize; --i >= 0; )
				{
					int l = len_t[i] & unchecked((int)(0xff));
					if (l > maxLen)
					{
						maxLen = l;
					}
					if (l < minLen)
					{
						minLen = l;
					}
				}
				// assert (maxLen <= 20) : maxLen;
				// assert (minLen >= 1) : minLen;
				HbAssignCodes(code[t], len[t], minLen, maxLen, alphaSize);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendMTFValues4()
		{
			bool[] inUse = this.data.inUse;
			bool[] inUse16 = this.data.sentMTFValues4_inUse16;
			for (int i = 16; --i >= 0; )
			{
				inUse16[i] = false;
				int i16 = i * 16;
				for (int j = 16; --j >= 0; )
				{
					if (inUse[i16 + j])
					{
						inUse16[i] = true;
					}
				}
			}
			for (int i_1 = 0; i_1 < 16; i_1++)
			{
				BsW(1, inUse16[i_1] ? 1 : 0);
			}
			OutputStream outShadow = this.@out;
			int bsLiveShadow = this.bsLive;
			int bsBuffShadow = this.bsBuff;
			for (int i_2 = 0; i_2 < 16; i_2++)
			{
				if (inUse16[i_2])
				{
					int i16 = i_2 * 16;
					for (int j = 0; j < 16; j++)
					{
						// inlined: bsW(1, inUse[i16 + j] ? 1 : 0);
						while (bsLiveShadow >= 8)
						{
							outShadow.Write(bsBuffShadow >> 24);
							// write 8-bit
							bsBuffShadow <<= 8;
							bsLiveShadow -= 8;
						}
						if (inUse[i16 + j])
						{
							bsBuffShadow |= 1 << (32 - bsLiveShadow - 1);
						}
						bsLiveShadow++;
					}
				}
			}
			this.bsBuff = bsBuffShadow;
			this.bsLive = bsLiveShadow;
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendMTFValues5(int nGroups, int nSelectors)
		{
			BsW(3, nGroups);
			BsW(15, nSelectors);
			OutputStream outShadow = this.@out;
			byte[] selectorMtf = this.data.selectorMtf;
			int bsLiveShadow = this.bsLive;
			int bsBuffShadow = this.bsBuff;
			for (int i = 0; i < nSelectors; i++)
			{
				for (int j = 0; j < hj; j++)
				{
					// inlined: bsW(1, 1);
					while (bsLiveShadow >= 8)
					{
						outShadow.Write(bsBuffShadow >> 24);
						bsBuffShadow <<= 8;
						bsLiveShadow -= 8;
					}
					bsBuffShadow |= 1 << (32 - bsLiveShadow - 1);
					bsLiveShadow++;
				}
				// inlined: bsW(1, 0);
				while (bsLiveShadow >= 8)
				{
					outShadow.Write(bsBuffShadow >> 24);
					bsBuffShadow <<= 8;
					bsLiveShadow -= 8;
				}
				// bsBuffShadow |= 0 << (32 - bsLiveShadow - 1);
				bsLiveShadow++;
			}
			this.bsBuff = bsBuffShadow;
			this.bsLive = bsLiveShadow;
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendMTFValues6(int nGroups, int alphaSize)
		{
			byte[][] len = this.data.sendMTFValues_len;
			OutputStream outShadow = this.@out;
			int bsLiveShadow = this.bsLive;
			int bsBuffShadow = this.bsBuff;
			for (int t = 0; t < nGroups; t++)
			{
				byte[] len_t = len[t];
				int curr = len_t[0] & unchecked((int)(0xff));
				// inlined: bsW(5, curr);
				while (bsLiveShadow >= 8)
				{
					outShadow.Write(bsBuffShadow >> 24);
					// write 8-bit
					bsBuffShadow <<= 8;
					bsLiveShadow -= 8;
				}
				bsBuffShadow |= curr << (32 - bsLiveShadow - 5);
				bsLiveShadow += 5;
				for (int i = 0; i < alphaSize; i++)
				{
					int lti = len_t[i] & unchecked((int)(0xff));
					while (curr < lti)
					{
						// inlined: bsW(2, 2);
						while (bsLiveShadow >= 8)
						{
							outShadow.Write(bsBuffShadow >> 24);
							// write 8-bit
							bsBuffShadow <<= 8;
							bsLiveShadow -= 8;
						}
						bsBuffShadow |= 2 << (32 - bsLiveShadow - 2);
						bsLiveShadow += 2;
						curr++;
					}
					/* 10 */
					while (curr > lti)
					{
						// inlined: bsW(2, 3);
						while (bsLiveShadow >= 8)
						{
							outShadow.Write(bsBuffShadow >> 24);
							// write 8-bit
							bsBuffShadow <<= 8;
							bsLiveShadow -= 8;
						}
						bsBuffShadow |= 3 << (32 - bsLiveShadow - 2);
						bsLiveShadow += 2;
						curr--;
					}
					/* 11 */
					// inlined: bsW(1, 0);
					while (bsLiveShadow >= 8)
					{
						outShadow.Write(bsBuffShadow >> 24);
						// write 8-bit
						bsBuffShadow <<= 8;
						bsLiveShadow -= 8;
					}
					// bsBuffShadow |= 0 << (32 - bsLiveShadow - 1);
					bsLiveShadow++;
				}
			}
			this.bsBuff = bsBuffShadow;
			this.bsLive = bsLiveShadow;
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendMTFValues7(int nSelectors)
		{
			CBZip2OutputStream.Data dataShadow = this.data;
			byte[][] len = dataShadow.sendMTFValues_len;
			int[][] code = dataShadow.sendMTFValues_code;
			OutputStream outShadow = this.@out;
			byte[] selector = dataShadow.selector;
			char[] sfmap = dataShadow.sfmap;
			int nMTFShadow = this.nMTF;
			int selCtr = 0;
			int bsLiveShadow = this.bsLive;
			int bsBuffShadow = this.bsBuff;
			for (int gs = 0; gs < nMTFShadow; )
			{
				int ge = Math.Min(gs + GSize - 1, nMTFShadow - 1);
				int selector_selCtr = selector[selCtr] & unchecked((int)(0xff));
				int[] code_selCtr = code[selector_selCtr];
				byte[] len_selCtr = len[selector_selCtr];
				while (gs <= ge)
				{
					int sfmap_i = sfmap[gs];
					//
					// inlined: bsW(len_selCtr[sfmap_i] & 0xff,
					// code_selCtr[sfmap_i]);
					//
					while (bsLiveShadow >= 8)
					{
						outShadow.Write(bsBuffShadow >> 24);
						bsBuffShadow <<= 8;
						bsLiveShadow -= 8;
					}
					int n = len_selCtr[sfmap_i] & unchecked((int)(0xFF));
					bsBuffShadow |= code_selCtr[sfmap_i] << (32 - bsLiveShadow - n);
					bsLiveShadow += n;
					gs++;
				}
				gs = ge + 1;
				selCtr++;
			}
			this.bsBuff = bsBuffShadow;
			this.bsLive = bsLiveShadow;
		}

		/// <exception cref="System.IO.IOException"/>
		private void MoveToFrontCodeAndSend()
		{
			BsW(24, this.origPtr);
			GenerateMTFValues();
			SendMTFValues();
		}

		/// <summary>This is the most hammered method of this class.</summary>
		/// <remarks>
		/// This is the most hammered method of this class.
		/// <p>
		/// This is the version using unrolled loops. Normally I never use such ones
		/// in Java code. The unrolling has shown a noticable performance improvement
		/// on JRE 1.4.2 (Linux i586 / HotSpot Client). Of course it depends on the
		/// JIT compiler of the vm.
		/// </p>
		/// </remarks>
		private bool MainSimpleSort(CBZip2OutputStream.Data dataShadow, int lo, int hi, int
			 d)
		{
			int bigN = hi - lo + 1;
			if (bigN < 2)
			{
				return this.firstAttempt && (this.workDone > this.workLimit);
			}
			int hp = 0;
			while (Incs[hp] < bigN)
			{
				hp++;
			}
			int[] fmap = dataShadow.fmap;
			char[] quadrant = dataShadow.quadrant;
			byte[] block = dataShadow.block;
			int lastShadow = this.last;
			int lastPlus1 = lastShadow + 1;
			bool firstAttemptShadow = this.firstAttempt;
			int workLimitShadow = this.workLimit;
			int workDoneShadow = this.workDone;
			// Following block contains unrolled code which could be shortened by
			// coding it in additional loops.
			while (--hp >= 0)
			{
				int h = Incs[hp];
				int mj = lo + h - 1;
				for (int i = lo + h; i <= hi; )
				{
					// copy
					for (int k = 3; (i <= hi) && (--k >= 0); i++)
					{
						int v = fmap[i];
						int vd = v + d;
						int j = i;
						// for (int a;
						// (j > mj) && mainGtU((a = fmap[j - h]) + d, vd,
						// block, quadrant, lastShadow);
						// j -= h) {
						// fmap[j] = a;
						// }
						//
						// unrolled version:
						// start inline mainGTU
						bool onceRunned = false;
						int a = 0;
						while (true)
						{
							if (onceRunned)
							{
								fmap[j] = a;
								if ((j -= h) <= mj)
								{
									goto HAMMER_break;
								}
							}
							else
							{
								onceRunned = true;
							}
							a = fmap[j - h];
							int i1 = a + d;
							int i2 = vd;
							// following could be done in a loop, but
							// unrolled it for performance:
							if (block[i1 + 1] == block[i2 + 1])
							{
								if (block[i1 + 2] == block[i2 + 2])
								{
									if (block[i1 + 3] == block[i2 + 3])
									{
										if (block[i1 + 4] == block[i2 + 4])
										{
											if (block[i1 + 5] == block[i2 + 5])
											{
												if (block[(i1 += 6)] == block[(i2 += 6)])
												{
													int x = lastShadow;
													while (x > 0)
													{
														x -= 4;
														if (block[i1 + 1] == block[i2 + 1])
														{
															if (quadrant[i1] == quadrant[i2])
															{
																if (block[i1 + 2] == block[i2 + 2])
																{
																	if (quadrant[i1 + 1] == quadrant[i2 + 1])
																	{
																		if (block[i1 + 3] == block[i2 + 3])
																		{
																			if (quadrant[i1 + 2] == quadrant[i2 + 2])
																			{
																				if (block[i1 + 4] == block[i2 + 4])
																				{
																					if (quadrant[i1 + 3] == quadrant[i2 + 3])
																					{
																						if ((i1 += 4) >= lastPlus1)
																						{
																							i1 -= lastPlus1;
																						}
																						if ((i2 += 4) >= lastPlus1)
																						{
																							i2 -= lastPlus1;
																						}
																						workDoneShadow++;
																						goto X_continue;
																					}
																					else
																					{
																						if ((quadrant[i1 + 3] > quadrant[i2 + 3]))
																						{
																							goto HAMMER_continue;
																						}
																						else
																						{
																							goto HAMMER_break;
																						}
																					}
																				}
																				else
																				{
																					if ((block[i1 + 4] & unchecked((int)(0xff))) > (block[i2 + 4] & unchecked((int)(0xff
																						))))
																					{
																						goto HAMMER_continue;
																					}
																					else
																					{
																						goto HAMMER_break;
																					}
																				}
																			}
																			else
																			{
																				if ((quadrant[i1 + 2] > quadrant[i2 + 2]))
																				{
																					goto HAMMER_continue;
																				}
																				else
																				{
																					goto HAMMER_break;
																				}
																			}
																		}
																		else
																		{
																			if ((block[i1 + 3] & unchecked((int)(0xff))) > (block[i2 + 3] & unchecked((int)(0xff
																				))))
																			{
																				goto HAMMER_continue;
																			}
																			else
																			{
																				goto HAMMER_break;
																			}
																		}
																	}
																	else
																	{
																		if ((quadrant[i1 + 1] > quadrant[i2 + 1]))
																		{
																			goto HAMMER_continue;
																		}
																		else
																		{
																			goto HAMMER_break;
																		}
																	}
																}
																else
																{
																	if ((block[i1 + 2] & unchecked((int)(0xff))) > (block[i2 + 2] & unchecked((int)(0xff
																		))))
																	{
																		goto HAMMER_continue;
																	}
																	else
																	{
																		goto HAMMER_break;
																	}
																}
															}
															else
															{
																if ((quadrant[i1] > quadrant[i2]))
																{
																	goto HAMMER_continue;
																}
																else
																{
																	goto HAMMER_break;
																}
															}
														}
														else
														{
															if ((block[i1 + 1] & unchecked((int)(0xff))) > (block[i2 + 1] & unchecked((int)(0xff
																))))
															{
																goto HAMMER_continue;
															}
															else
															{
																goto HAMMER_break;
															}
														}
X_continue: ;
													}
X_break: ;
													goto HAMMER_break;
												}
												else
												{
													// while x > 0
													if ((block[i1] & unchecked((int)(0xff))) > (block[i2] & unchecked((int)(0xff))))
													{
														goto HAMMER_continue;
													}
													else
													{
														goto HAMMER_break;
													}
												}
											}
											else
											{
												if ((block[i1 + 5] & unchecked((int)(0xff))) > (block[i2 + 5] & unchecked((int)(0xff
													))))
												{
													goto HAMMER_continue;
												}
												else
												{
													goto HAMMER_break;
												}
											}
										}
										else
										{
											if ((block[i1 + 4] & unchecked((int)(0xff))) > (block[i2 + 4] & unchecked((int)(0xff
												))))
											{
												goto HAMMER_continue;
											}
											else
											{
												goto HAMMER_break;
											}
										}
									}
									else
									{
										if ((block[i1 + 3] & unchecked((int)(0xff))) > (block[i2 + 3] & unchecked((int)(0xff
											))))
										{
											goto HAMMER_continue;
										}
										else
										{
											goto HAMMER_break;
										}
									}
								}
								else
								{
									if ((block[i1 + 2] & unchecked((int)(0xff))) > (block[i2 + 2] & unchecked((int)(0xff
										))))
									{
										goto HAMMER_continue;
									}
									else
									{
										goto HAMMER_break;
									}
								}
							}
							else
							{
								if ((block[i1 + 1] & unchecked((int)(0xff))) > (block[i2 + 1] & unchecked((int)(0xff
									))))
								{
									goto HAMMER_continue;
								}
								else
								{
									goto HAMMER_break;
								}
							}
HAMMER_continue: ;
						}
HAMMER_break: ;
						// HAMMER
						// end inline mainGTU
						fmap[j] = v;
					}
					if (firstAttemptShadow && (i <= hi) && (workDoneShadow > workLimitShadow))
					{
						goto HP_break;
					}
				}
HP_continue: ;
			}
HP_break: ;
			this.workDone = workDoneShadow;
			return firstAttemptShadow && (workDoneShadow > workLimitShadow);
		}

		private static void Vswap(int[] fmap, int p1, int p2, int n)
		{
			n += p1;
			while (p1 < n)
			{
				int t = fmap[p1];
				fmap[p1++] = fmap[p2];
				fmap[p2++] = t;
			}
		}

		private static byte Med3(byte a, byte b, byte c)
		{
			return (((sbyte)a) < b) ? (((sbyte)b) < c ? b : ((sbyte)a) < c ? c : a) : (b > c ? 
				b : a > c ? c : a);
		}

		private void BlockSort()
		{
			this.workLimit = WorkFactor * this.last;
			this.workDone = 0;
			this.blockRandomised = false;
			this.firstAttempt = true;
			MainSort();
			if (this.firstAttempt && (this.workDone > this.workLimit))
			{
				RandomiseBlock();
				this.workLimit = this.workDone = 0;
				this.firstAttempt = false;
				MainSort();
			}
			int[] fmap = this.data.fmap;
			this.origPtr = -1;
			for (int i = 0; i <= lastShadow; i++)
			{
				if (fmap[i] == 0)
				{
					this.origPtr = i;
					break;
				}
			}
		}

		// assert (this.origPtr != -1) : this.origPtr;
		/// <summary>Method "mainQSort3", file "blocksort.c", BZip2 1.0.2</summary>
		private void MainQSort3(CBZip2OutputStream.Data dataShadow, int loSt, int hiSt, int
			 dSt)
		{
			int[] stack_ll = dataShadow.stack_ll;
			int[] stack_hh = dataShadow.stack_hh;
			int[] stack_dd = dataShadow.stack_dd;
			int[] fmap = dataShadow.fmap;
			byte[] block = dataShadow.block;
			stack_ll[0] = loSt;
			stack_hh[0] = hiSt;
			stack_dd[0] = dSt;
			for (int sp = 1; --sp >= 0; )
			{
				int lo = stack_ll[sp];
				int hi = stack_hh[sp];
				int d = stack_dd[sp];
				if ((hi - lo < SmallThresh) || (d > DepthThresh))
				{
					if (MainSimpleSort(dataShadow, lo, hi, d))
					{
						return;
					}
				}
				else
				{
					int d1 = d + 1;
					int med = Med3(block[fmap[lo] + d1], block[fmap[hi] + d1], block[fmap[(int)(((uint
						)(lo + hi)) >> 1)] + d1]) & unchecked((int)(0xff));
					int unLo = lo;
					int unHi = hi;
					int ltLo = lo;
					int gtHi = hi;
					while (true)
					{
						while (unLo <= unHi)
						{
							int n = ((int)block[fmap[unLo] + d1] & unchecked((int)(0xff))) - med;
							if (n == 0)
							{
								int temp = fmap[unLo];
								fmap[unLo++] = fmap[ltLo];
								fmap[ltLo++] = temp;
							}
							else
							{
								if (n < 0)
								{
									unLo++;
								}
								else
								{
									break;
								}
							}
						}
						while (unLo <= unHi)
						{
							int n = ((int)block[fmap[unHi] + d1] & unchecked((int)(0xff))) - med;
							if (n == 0)
							{
								int temp = fmap[unHi];
								fmap[unHi--] = fmap[gtHi];
								fmap[gtHi--] = temp;
							}
							else
							{
								if (n > 0)
								{
									unHi--;
								}
								else
								{
									break;
								}
							}
						}
						if (unLo <= unHi)
						{
							int temp = fmap[unLo];
							fmap[unLo++] = fmap[unHi];
							fmap[unHi--] = temp;
						}
						else
						{
							break;
						}
					}
					if (gtHi < ltLo)
					{
						stack_ll[sp] = lo;
						stack_hh[sp] = hi;
						stack_dd[sp] = d1;
						sp++;
					}
					else
					{
						int n = ((ltLo - lo) < (unLo - ltLo)) ? (ltLo - lo) : (unLo - ltLo);
						Vswap(fmap, lo, unLo - n, n);
						int m = ((hi - gtHi) < (gtHi - unHi)) ? (hi - gtHi) : (gtHi - unHi);
						Vswap(fmap, unLo, hi - m + 1, m);
						n = lo + unLo - ltLo - 1;
						m = hi - (gtHi - unHi) + 1;
						stack_ll[sp] = lo;
						stack_hh[sp] = n;
						stack_dd[sp] = d;
						sp++;
						stack_ll[sp] = n + 1;
						stack_hh[sp] = m - 1;
						stack_dd[sp] = d1;
						sp++;
						stack_ll[sp] = m;
						stack_hh[sp] = hi;
						stack_dd[sp] = d;
						sp++;
					}
				}
			}
		}

		private void MainSort()
		{
			CBZip2OutputStream.Data dataShadow = this.data;
			int[] runningOrder = dataShadow.mainSort_runningOrder;
			int[] copy = dataShadow.mainSort_copy;
			bool[] bigDone = dataShadow.mainSort_bigDone;
			int[] ftab = dataShadow.ftab;
			byte[] block = dataShadow.block;
			int[] fmap = dataShadow.fmap;
			char[] quadrant = dataShadow.quadrant;
			int lastShadow = this.last;
			int workLimitShadow = this.workLimit;
			bool firstAttemptShadow = this.firstAttempt;
			// Set up the 2-byte frequency table
			for (int i = 65537; --i >= 0; )
			{
				ftab[i] = 0;
			}
			/*
			* In the various block-sized structures, live data runs from 0 to
			* last+NUM_OVERSHOOT_BYTES inclusive. First, set up the overshoot area
			* for block.
			*/
			for (int i_1 = 0; i_1 < NumOvershootBytes; i_1++)
			{
				block[lastShadow + i_1 + 2] = block[(i_1 % (lastShadow + 1)) + 1];
			}
			for (int i_2 = lastShadow + NumOvershootBytes + 1; --i_2 >= 0; )
			{
				quadrant[i_2] = (char)0;
			}
			block[0] = block[lastShadow + 1];
			// Complete the initial radix sort:
			int c1 = block[0] & unchecked((int)(0xff));
			for (int i_3 = 0; i_3 <= lastShadow; i_3++)
			{
				int c2 = block[i_3 + 1] & unchecked((int)(0xff));
				ftab[(c1 << 8) + c2]++;
				c1 = c2;
			}
			for (int i_4 = 1; i_4 <= 65536; i_4++)
			{
				ftab[i_4] += ftab[i_4 - 1];
			}
			c1 = block[1] & unchecked((int)(0xff));
			for (int i_5 = 0; i_5 < lastShadow; i_5++)
			{
				int c2 = block[i_5 + 2] & unchecked((int)(0xff));
				fmap[--ftab[(c1 << 8) + c2]] = i_5;
				c1 = c2;
			}
			fmap[--ftab[((block[lastShadow + 1] & unchecked((int)(0xff))) << 8) + (block[1] &
				 unchecked((int)(0xff)))]] = lastShadow;
			/*
			* Now ftab contains the first loc of every small bucket. Calculate the
			* running order, from smallest to largest big bucket.
			*/
			for (int i_6 = 256; --i_6 >= 0; )
			{
				bigDone[i_6] = false;
				runningOrder[i_6] = i_6;
			}
			for (int h = 364; h != 1; )
			{
				h /= 3;
				for (int i_7 = h; i_7 <= 255; i_7++)
				{
					int vv = runningOrder[i_7];
					int a = ftab[(vv + 1) << 8] - ftab[vv << 8];
					int b = h - 1;
					int j = i_7;
					for (int ro = runningOrder[j - h]; (ftab[(ro + 1) << 8] - ftab[ro << 8]) > a; ro 
						= runningOrder[j - h])
					{
						runningOrder[j] = ro;
						j -= h;
						if (j <= b)
						{
							break;
						}
					}
					runningOrder[j] = vv;
				}
			}
			/*
			* The main sorting loop.
			*/
			for (int i_8 = 0; i_8 <= 255; i_8++)
			{
				/*
				* Process big buckets, starting with the least full.
				*/
				int ss = runningOrder[i_8];
				// Step 1:
				/*
				* Complete the big bucket [ss] by quicksorting any unsorted small
				* buckets [ss, j]. Hopefully previous pointer-scanning phases have
				* already completed many of the small buckets [ss, j], so we don't
				* have to sort them at all.
				*/
				for (int j = 0; j <= 255; j++)
				{
					int sb = (ss << 8) + j;
					int ftab_sb = ftab[sb];
					if ((ftab_sb & Setmask) != Setmask)
					{
						int lo = ftab_sb & Clearmask;
						int hi = (ftab[sb + 1] & Clearmask) - 1;
						if (hi > lo)
						{
							MainQSort3(dataShadow, lo, hi, 2);
							if (firstAttemptShadow && (this.workDone > workLimitShadow))
							{
								return;
							}
						}
						ftab[sb] = ftab_sb | Setmask;
					}
				}
				// Step 2:
				// Now scan this big bucket so as to synthesise the
				// sorted order for small buckets [t, ss] for all t != ss.
				for (int j_1 = 0; j_1 <= 255; j_1++)
				{
					copy[j_1] = ftab[(j_1 << 8) + ss] & Clearmask;
				}
				for (int j_2 = ftab[ss << 8] & Clearmask; j_2 < hj; j_2++)
				{
					int fmap_j = fmap[j_2];
					c1 = block[fmap_j] & unchecked((int)(0xff));
					if (!bigDone[c1])
					{
						fmap[copy[c1]] = (fmap_j == 0) ? lastShadow : (fmap_j - 1);
						copy[c1]++;
					}
				}
				for (int j_3 = 256; --j_3 >= 0; )
				{
					ftab[(j_3 << 8) + ss] |= Setmask;
				}
				// Step 3:
				/*
				* The ss big bucket is now done. Record this fact, and update the
				* quadrant descriptors. Remember to update quadrants in the
				* overshoot area too, if necessary. The "if (i < 255)" test merely
				* skips this updating for the last bucket processed, since updating
				* for the last bucket is pointless.
				*/
				bigDone[ss] = true;
				if (i_8 < 255)
				{
					int bbStart = ftab[ss << 8] & Clearmask;
					int bbSize = (ftab[(ss + 1) << 8] & Clearmask) - bbStart;
					int shifts = 0;
					while ((bbSize >> shifts) > 65534)
					{
						shifts++;
					}
					for (int j_4 = 0; j_4 < bbSize; j_4++)
					{
						int a2update = fmap[bbStart + j_4];
						char qVal = (char)(j_4 >> shifts);
						quadrant[a2update] = qVal;
						if (a2update < NumOvershootBytes)
						{
							quadrant[a2update + lastShadow + 1] = qVal;
						}
					}
				}
			}
		}

		private void RandomiseBlock()
		{
			bool[] inUse = this.data.inUse;
			byte[] block = this.data.block;
			int lastShadow = this.last;
			for (int i = 256; --i >= 0; )
			{
				inUse[i] = false;
			}
			int rNToGo = 0;
			int rTPos = 0;
			for (int i_1 = 0; i_1 <= lastShadow; i_1 = j, j++)
			{
				if (rNToGo == 0)
				{
					rNToGo = (char)BZip2Constants.rNums[rTPos];
					if (++rTPos == 512)
					{
						rTPos = 0;
					}
				}
				rNToGo--;
				block[j] ^= ((rNToGo == 1) ? 1 : 0);
				// handle 16 bit signed numbers
				inUse[block[j] & unchecked((int)(0xff))] = true;
			}
			this.blockRandomised = true;
		}

		private void GenerateMTFValues()
		{
			int lastShadow = this.last;
			CBZip2OutputStream.Data dataShadow = this.data;
			bool[] inUse = dataShadow.inUse;
			byte[] block = dataShadow.block;
			int[] fmap = dataShadow.fmap;
			char[] sfmap = dataShadow.sfmap;
			int[] mtfFreq = dataShadow.mtfFreq;
			byte[] unseqToSeq = dataShadow.unseqToSeq;
			byte[] yy = dataShadow.generateMTFValues_yy;
			// make maps
			int nInUseShadow = 0;
			for (int i = 0; i < 256; i++)
			{
				if (inUse[i])
				{
					unseqToSeq[i] = unchecked((byte)nInUseShadow);
					nInUseShadow++;
				}
			}
			this.nInUse = nInUseShadow;
			int eob = nInUseShadow + 1;
			for (int i_1 = eob; i_1 >= 0; i_1--)
			{
				mtfFreq[i_1] = 0;
			}
			for (int i_2 = nInUseShadow; --i_2 >= 0; )
			{
				yy[i_2] = unchecked((byte)i_2);
			}
			int wr = 0;
			int zPend = 0;
			for (int i_3 = 0; i_3 <= lastShadow; i_3++)
			{
				byte ll_i = unseqToSeq[block[fmap[i_3]] & unchecked((int)(0xff))];
				byte tmp = yy[0];
				int j = 0;
				while (ll_i != tmp)
				{
					j++;
					byte tmp2 = tmp;
					tmp = yy[j];
					yy[j] = tmp2;
				}
				yy[0] = tmp;
				if (j == 0)
				{
					zPend++;
				}
				else
				{
					if (zPend > 0)
					{
						zPend--;
						while (true)
						{
							if ((zPend & 1) == 0)
							{
								sfmap[wr] = (char)Runa;
								wr++;
								mtfFreq[Runa]++;
							}
							else
							{
								sfmap[wr] = (char)Runb;
								wr++;
								mtfFreq[Runb]++;
							}
							if (zPend >= 2)
							{
								zPend = (zPend - 2) >> 1;
							}
							else
							{
								break;
							}
						}
						zPend = 0;
					}
					sfmap[wr] = (char)(j + 1);
					wr++;
					mtfFreq[j + 1]++;
				}
			}
			if (zPend > 0)
			{
				zPend--;
				while (true)
				{
					if ((zPend & 1) == 0)
					{
						sfmap[wr] = (char)Runa;
						wr++;
						mtfFreq[Runa]++;
					}
					else
					{
						sfmap[wr] = (char)Runb;
						wr++;
						mtfFreq[Runb]++;
					}
					if (zPend >= 2)
					{
						zPend = (zPend - 2) >> 1;
					}
					else
					{
						break;
					}
				}
			}
			sfmap[wr] = (char)eob;
			mtfFreq[eob]++;
			this.nMTF = wr + 1;
		}

		private sealed class Data : object
		{
			internal readonly bool[] inUse = new bool[256];

			internal readonly byte[] unseqToSeq = new byte[256];

			internal readonly int[] mtfFreq = new int[MaxAlphaSize];

			internal readonly byte[] selector = new byte[MaxSelectors];

			internal readonly byte[] selectorMtf = new byte[MaxSelectors];

			internal readonly byte[] generateMTFValues_yy = new byte[256];

			internal readonly byte[][] sendMTFValues_len = new byte[][] { new byte[MaxAlphaSize
				], new byte[MaxAlphaSize], new byte[MaxAlphaSize], new byte[MaxAlphaSize], new byte
				[MaxAlphaSize], new byte[MaxAlphaSize] };

			internal readonly int[][] sendMTFValues_rfreq = new int[][] { new int[MaxAlphaSize
				], new int[MaxAlphaSize], new int[MaxAlphaSize], new int[MaxAlphaSize], new int[
				MaxAlphaSize], new int[MaxAlphaSize] };

			internal readonly int[] sendMTFValues_fave = new int[NGroups];

			internal readonly short[] sendMTFValues_cost = new short[NGroups];

			internal readonly int[][] sendMTFValues_code = new int[][] { new int[MaxAlphaSize
				], new int[MaxAlphaSize], new int[MaxAlphaSize], new int[MaxAlphaSize], new int[
				MaxAlphaSize], new int[MaxAlphaSize] };

			internal readonly byte[] sendMTFValues2_pos = new byte[NGroups];

			internal readonly bool[] sentMTFValues4_inUse16 = new bool[16];

			internal readonly int[] stack_ll = new int[QsortStackSize];

			internal readonly int[] stack_hh = new int[QsortStackSize];

			internal readonly int[] stack_dd = new int[QsortStackSize];

			internal readonly int[] mainSort_runningOrder = new int[256];

			internal readonly int[] mainSort_copy = new int[256];

			internal readonly bool[] mainSort_bigDone = new bool[256];

			internal readonly int[] heap = new int[MaxAlphaSize + 2];

			internal readonly int[] weight = new int[MaxAlphaSize * 2];

			internal readonly int[] parent = new int[MaxAlphaSize * 2];

			internal readonly int[] ftab = new int[65537];

			internal readonly byte[] block;

			internal readonly int[] fmap;

			internal readonly char[] sfmap;

			/// <summary>
			/// Array instance identical to sfmap, both are used only temporarily and
			/// indepently, so we do not need to allocate additional memory.
			/// </summary>
			internal readonly char[] quadrant;

			internal Data(int blockSize100k)
				: base()
			{
				// with blockSize 900k
				// 256 byte
				// 256 byte
				// 1032 byte
				// 18002 byte
				// 18002 byte
				// 256 byte
				// 1548
				// byte
				// 6192
				// byte
				// 24 byte
				// 12 byte
				// 6192
				// byte
				// 6 byte
				// 16 byte
				// 4000 byte
				// 4000 byte
				// 4000 byte
				// 1024 byte
				// 1024 byte
				// 256 byte
				// 1040 byte
				// 2064 byte
				// 2064 byte
				// 262148 byte
				// ------------
				// 333408 byte
				// 900021 byte
				// 3600000 byte
				// 3600000 byte
				// ------------
				// 8433529 byte
				// ============
				int n = blockSize100k * BZip2Constants.baseBlockSize;
				this.block = new byte[(n + 1 + NumOvershootBytes)];
				this.fmap = new int[n];
				this.sfmap = new char[2 * n];
				this.quadrant = this.sfmap;
			}
		}
	}
}
