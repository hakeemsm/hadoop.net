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

namespace org.apache.hadoop.io.compress
{
	public class CompressDecompressTester<T, E>
		where T : org.apache.hadoop.io.compress.Compressor
		where E : org.apache.hadoop.io.compress.Decompressor
	{
		private static readonly org.apache.log4j.Logger logger = org.apache.log4j.Logger.
			getLogger(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.CompressDecompressTester
			)));

		private readonly byte[] originalRawData;

		private com.google.common.collect.ImmutableList<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
			<T, E>> pairs = com.google.common.collect.ImmutableList.of();

		private com.google.common.collect.ImmutableList.Builder<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
			<T, E>> builder = com.google.common.collect.ImmutableList.builder();

		private com.google.common.collect.ImmutableSet<org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
			> stateges = com.google.common.collect.ImmutableSet.of();

		private org.apache.hadoop.io.compress.CompressDecompressTester.PreAssertionTester
			<T, E> assertionDelegate;

		public CompressDecompressTester(byte[] originalRawData)
		{
			this.originalRawData = java.util.Arrays.copyOf(originalRawData, originalRawData.Length
				);
			this.assertionDelegate = new _PreAssertionTester_65();
		}

		private sealed class _PreAssertionTester_65 : org.apache.hadoop.io.compress.CompressDecompressTester.PreAssertionTester
			<T, E>
		{
			public _PreAssertionTester_65()
			{
			}

			public com.google.common.collect.ImmutableList<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
				<T, E>> filterOnAssumeWhat(com.google.common.collect.ImmutableList<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
				<T, E>> pairs)
			{
				com.google.common.collect.ImmutableList.Builder<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
					<T, E>> builder = com.google.common.collect.ImmutableList.builder();
				foreach (org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair<T, E> 
					pair in pairs)
				{
					if (org.apache.hadoop.io.compress.CompressDecompressTester.isAvailable(pair))
					{
						builder.add(pair);
					}
				}
				return ((com.google.common.collect.ImmutableList<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
					<T, E>>)builder.build());
			}
		}

		private static bool isNativeSnappyLoadable()
		{
			bool snappyAvailable = false;
			bool loaded = false;
			try
			{
				Sharpen.Runtime.loadLibrary("snappy");
				logger.warn("Snappy native library is available");
				snappyAvailable = true;
				bool hadoopNativeAvailable = org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded
					();
				loaded = snappyAvailable && hadoopNativeAvailable;
				if (loaded)
				{
					logger.info("Snappy native library loaded");
				}
				else
				{
					logger.warn("Snappy native library not loaded");
				}
			}
			catch (System.Exception t)
			{
				logger.warn("Failed to load snappy: ", t);
				return false;
			}
			return loaded;
		}

		public static org.apache.hadoop.io.compress.CompressDecompressTester<T, E> of<T, 
			E>(byte[] rawData)
			where T : org.apache.hadoop.io.compress.Compressor
			where E : org.apache.hadoop.io.compress.Decompressor
		{
			return new org.apache.hadoop.io.compress.CompressDecompressTester<T, E>(rawData);
		}

		public virtual org.apache.hadoop.io.compress.CompressDecompressTester<T, E> withCompressDecompressPair
			(T compressor, E decompressor)
		{
			addPair(compressor, decompressor, com.google.common.@base.Joiner.on("_").join(Sharpen.Runtime.getClassForObject
				(compressor).getCanonicalName(), Sharpen.Runtime.getClassForObject(decompressor)
				.getCanonicalName()));
			return this;
		}

		public virtual org.apache.hadoop.io.compress.CompressDecompressTester<T, E> withTestCases
			(com.google.common.collect.ImmutableSet<org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
			> stateges)
		{
			this.stateges = com.google.common.collect.ImmutableSet.copyOf(stateges);
			return this;
		}

		private void addPair(T compressor, E decompressor, string name)
		{
			builder.add(new org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
				<T, E>(name, compressor, decompressor));
		}

		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		public virtual void test()
		{
			pairs = ((com.google.common.collect.ImmutableList<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
				<T, E>>)builder.build());
			pairs = assertionDelegate.filterOnAssumeWhat(pairs);
			foreach (org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair<T, E> 
				pair in pairs)
			{
				foreach (org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					 strategy in stateges)
				{
					strategy.getTesterStrategy().assertCompression(pair.getName(), pair.getCompressor
						(), pair.getDecompressor(), java.util.Arrays.copyOf(originalRawData, originalRawData
						.Length));
				}
			}
			endAll(pairs);
		}

		private void endAll(com.google.common.collect.ImmutableList<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
			<T, E>> pairs)
		{
			foreach (org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair<T, E> 
				pair in pairs)
			{
				pair.end();
			}
		}

		internal interface PreAssertionTester<T, E>
			where T : org.apache.hadoop.io.compress.Compressor
			where E : org.apache.hadoop.io.compress.Decompressor
		{
			com.google.common.collect.ImmutableList<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
				<T, E>> filterOnAssumeWhat(com.google.common.collect.ImmutableList<org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
				<T, E>> pairs);
		}

		[System.Serializable]
		public sealed class CompressionTestStrategy
		{
			private sealed class _TesterCompressionStrategy_155 : org.apache.hadoop.io.compress.CompressDecompressTester.TesterCompressionStrategy
			{
				public _TesterCompressionStrategy_155()
				{
					this.joiner = com.google.common.@base.Joiner.on("- ");
				}

				private readonly com.google.common.@base.Joiner joiner;

				internal override void assertCompression(string name, org.apache.hadoop.io.compress.Compressor
					 compressor, org.apache.hadoop.io.compress.Decompressor decompressor, byte[] rawData
					)
				{
					NUnit.Framework.Assert.IsTrue(this.checkSetInputNullPointerException(compressor));
					NUnit.Framework.Assert.IsTrue(this.checkSetInputNullPointerException(decompressor
						));
					NUnit.Framework.Assert.IsTrue(this.checkCompressArrayIndexOutOfBoundsException(compressor
						, rawData));
					NUnit.Framework.Assert.IsTrue(this.checkCompressArrayIndexOutOfBoundsException(decompressor
						, rawData));
					NUnit.Framework.Assert.IsTrue(this.checkCompressNullPointerException(compressor, 
						rawData));
					NUnit.Framework.Assert.IsTrue(this.checkCompressNullPointerException(decompressor
						, rawData));
					NUnit.Framework.Assert.IsTrue(this.checkSetInputArrayIndexOutOfBoundsException(compressor
						));
					NUnit.Framework.Assert.IsTrue(this.checkSetInputArrayIndexOutOfBoundsException(decompressor
						));
				}

				private bool checkSetInputNullPointerException(org.apache.hadoop.io.compress.Compressor
					 compressor)
				{
					try
					{
						compressor.setInput(null, 0, 1);
					}
					catch (System.ArgumentNullException)
					{
						return true;
					}
					catch (System.Exception)
					{
						this.logger.error(this.joiner.join(Sharpen.Runtime.getClassForObject(compressor).
							getCanonicalName(), "checkSetInputNullPointerException error !!!"));
					}
					return false;
				}

				private bool checkCompressNullPointerException(org.apache.hadoop.io.compress.Compressor
					 compressor, byte[] rawData)
				{
					try
					{
						compressor.setInput(rawData, 0, rawData.Length);
						compressor.compress(null, 0, 1);
					}
					catch (System.ArgumentNullException)
					{
						return true;
					}
					catch (System.Exception)
					{
						this.logger.error(this.joiner.join(Sharpen.Runtime.getClassForObject(compressor).
							getCanonicalName(), "checkCompressNullPointerException error !!!"));
					}
					return false;
				}

				private bool checkCompressNullPointerException(org.apache.hadoop.io.compress.Decompressor
					 decompressor, byte[] rawData)
				{
					try
					{
						decompressor.setInput(rawData, 0, rawData.Length);
						decompressor.decompress(null, 0, 1);
					}
					catch (System.ArgumentNullException)
					{
						return true;
					}
					catch (System.Exception)
					{
						this.logger.error(this.joiner.join(Sharpen.Runtime.getClassForObject(decompressor
							).getCanonicalName(), "checkCompressNullPointerException error !!!"));
					}
					return false;
				}

				private bool checkSetInputNullPointerException(org.apache.hadoop.io.compress.Decompressor
					 decompressor)
				{
					try
					{
						decompressor.setInput(null, 0, 1);
					}
					catch (System.ArgumentNullException)
					{
						return true;
					}
					catch (System.Exception)
					{
						this.logger.error(this.joiner.join(Sharpen.Runtime.getClassForObject(decompressor
							).getCanonicalName(), "checkSetInputNullPointerException error !!!"));
					}
					return false;
				}

				private bool checkSetInputArrayIndexOutOfBoundsException(org.apache.hadoop.io.compress.Compressor
					 compressor)
				{
					try
					{
						compressor.setInput(new byte[] { unchecked((byte)0) }, 0, -1);
					}
					catch (System.IndexOutOfRangeException)
					{
						return true;
					}
					catch (System.Exception)
					{
						this.logger.error(this.joiner.join(Sharpen.Runtime.getClassForObject(compressor).
							getCanonicalName(), "checkSetInputArrayIndexOutOfBoundsException error !!!"));
					}
					return false;
				}

				private bool checkCompressArrayIndexOutOfBoundsException(org.apache.hadoop.io.compress.Compressor
					 compressor, byte[] rawData)
				{
					try
					{
						compressor.setInput(rawData, 0, rawData.Length);
						compressor.compress(new byte[rawData.Length], 0, -1);
					}
					catch (System.IndexOutOfRangeException)
					{
						return true;
					}
					catch (System.Exception)
					{
						this.logger.error(this.joiner.join(Sharpen.Runtime.getClassForObject(compressor).
							getCanonicalName(), "checkCompressArrayIndexOutOfBoundsException error !!!"));
					}
					return false;
				}

				private bool checkCompressArrayIndexOutOfBoundsException(org.apache.hadoop.io.compress.Decompressor
					 decompressor, byte[] rawData)
				{
					try
					{
						decompressor.setInput(rawData, 0, rawData.Length);
						decompressor.decompress(new byte[rawData.Length], 0, -1);
					}
					catch (System.IndexOutOfRangeException)
					{
						return true;
					}
					catch (System.Exception)
					{
						this.logger.error(this.joiner.join(Sharpen.Runtime.getClassForObject(decompressor
							).getCanonicalName(), "checkCompressArrayIndexOutOfBoundsException error !!!"));
					}
					return false;
				}

				private bool checkSetInputArrayIndexOutOfBoundsException(org.apache.hadoop.io.compress.Decompressor
					 decompressor)
				{
					try
					{
						decompressor.setInput(new byte[] { unchecked((byte)0) }, 0, -1);
					}
					catch (System.IndexOutOfRangeException)
					{
						return true;
					}
					catch (System.Exception)
					{
						this.logger.error(this.joiner.join(Sharpen.Runtime.getClassForObject(decompressor
							).getCanonicalName(), "checkNullPointerException error !!!"));
					}
					return false;
				}
			}

			public static readonly org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
				 COMPRESS_DECOMPRESS_ERRORS = new org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
				(new _TesterCompressionStrategy_155());

			private sealed class _TesterCompressionStrategy_285 : org.apache.hadoop.io.compress.CompressDecompressTester.TesterCompressionStrategy
			{
				public _TesterCompressionStrategy_285()
				{
					this.joiner = com.google.common.@base.Joiner.on("- ");
				}

				internal readonly com.google.common.@base.Joiner joiner;

				internal override void assertCompression(string name, org.apache.hadoop.io.compress.Compressor
					 compressor, org.apache.hadoop.io.compress.Decompressor decompressor, byte[] rawData
					)
				{
					int cSize = 0;
					int decompressedSize = 0;
					byte[] compressedResult = new byte[rawData.Length];
					byte[] decompressedBytes = new byte[rawData.Length];
					try
					{
						NUnit.Framework.Assert.IsTrue(this.joiner.join(name, "compressor.needsInput before error !!!"
							), compressor.needsInput());
						NUnit.Framework.Assert.IsTrue(this.joiner.join(name, "compressor.getBytesWritten before error !!!"
							), compressor.getBytesWritten() == 0);
						compressor.setInput(rawData, 0, rawData.Length);
						compressor.finish();
						while (!compressor.finished())
						{
							cSize += compressor.compress(compressedResult, 0, compressedResult.Length);
						}
						compressor.reset();
						NUnit.Framework.Assert.IsTrue(this.joiner.join(name, "decompressor.needsInput() before error !!!"
							), decompressor.needsInput());
						decompressor.setInput(compressedResult, 0, cSize);
						NUnit.Framework.Assert.IsFalse(this.joiner.join(name, "decompressor.needsInput() after error !!!"
							), decompressor.needsInput());
						while (!decompressor.finished())
						{
							decompressedSize = decompressor.decompress(decompressedBytes, 0, decompressedBytes
								.Length);
						}
						decompressor.reset();
						NUnit.Framework.Assert.IsTrue(this.joiner.join(name, " byte size not equals error !!!"
							), decompressedSize == rawData.Length);
						NUnit.Framework.Assert.assertArrayEquals(this.joiner.join(name, " byte arrays not equals error !!!"
							), rawData, decompressedBytes);
					}
					catch (System.Exception ex)
					{
						NUnit.Framework.Assert.Fail(this.joiner.join(name, ex.Message));
					}
				}
			}

			public static readonly org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
				 COMPRESS_DECOMPRESS_SINGLE_BLOCK = new org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
				(new _TesterCompressionStrategy_285());

			private sealed class _TesterCompressionStrategy_334 : org.apache.hadoop.io.compress.CompressDecompressTester.TesterCompressionStrategy
			{
				public _TesterCompressionStrategy_334()
				{
					this.joiner = com.google.common.@base.Joiner.on("- ");
					this.emptySize = com.google.common.collect.ImmutableMap.of(Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.io.compress.lz4.Lz4Compressor)), 4, Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.io.compress.zlib.ZlibCompressor)), 16, Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.io.compress.snappy.SnappyCompressor)), 4, Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater)), 16);
				}

				internal readonly com.google.common.@base.Joiner joiner;

				internal readonly com.google.common.collect.ImmutableMap<java.lang.Class, int> emptySize;

				internal override void assertCompression(string name, org.apache.hadoop.io.compress.Compressor
					 compressor, org.apache.hadoop.io.compress.Decompressor decompressor, byte[] originalRawData
					)
				{
					byte[] buf = null;
					java.io.ByteArrayInputStream bytesIn = null;
					org.apache.hadoop.io.compress.BlockDecompressorStream blockDecompressorStream = null;
					java.io.ByteArrayOutputStream bytesOut = new java.io.ByteArrayOutputStream();
					// close without write
					try
					{
						compressor.reset();
						// decompressor.end();
						org.apache.hadoop.io.compress.BlockCompressorStream blockCompressorStream = new org.apache.hadoop.io.compress.BlockCompressorStream
							(bytesOut, compressor, 1024, 0);
						blockCompressorStream.close();
						// check compressed output
						buf = bytesOut.toByteArray();
						int emSize = this.emptySize[Sharpen.Runtime.getClassForObject(compressor)];
						NUnit.Framework.Assert.AreEqual(this.joiner.join(name, "empty stream compressed output size != "
							 + emSize), emSize, buf.Length);
						// use compressed output as input for decompression
						bytesIn = new java.io.ByteArrayInputStream(buf);
						// create decompression stream
						blockDecompressorStream = new org.apache.hadoop.io.compress.BlockDecompressorStream
							(bytesIn, decompressor, 1024);
						// no byte is available because stream was closed
						NUnit.Framework.Assert.AreEqual(this.joiner.join(name, " return value is not -1")
							, -1, blockDecompressorStream.read());
					}
					catch (System.IO.IOException e)
					{
						NUnit.Framework.Assert.Fail(this.joiner.join(name, e.Message));
					}
					finally
					{
						if (blockDecompressorStream != null)
						{
							try
							{
								bytesOut.close();
								blockDecompressorStream.close();
								bytesIn.close();
								blockDecompressorStream.close();
							}
							catch (System.IO.IOException)
							{
							}
						}
					}
				}
			}

			public static readonly org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
				 COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM = new org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
				(new _TesterCompressionStrategy_334());

			private sealed class _TesterCompressionStrategy_384 : org.apache.hadoop.io.compress.CompressDecompressTester.TesterCompressionStrategy
			{
				public _TesterCompressionStrategy_384()
				{
					this.joiner = com.google.common.@base.Joiner.on("- ");
					this.BLOCK_SIZE = 512;
					this.operationBlock = new byte[_T1233992766.BLOCK_SIZE];
					this.overheadSpace = _T1233992766.BLOCK_SIZE / 100 + 12;
				}

				private readonly com.google.common.@base.Joiner joiner;

				private const int BLOCK_SIZE;

				private readonly byte[] operationBlock;

				private const int overheadSpace;

				// Use default of 512 as bufferSize and compressionOverhead of
				// (1% of bufferSize + 12 bytes) = 18 bytes (zlib algorithm).
				internal override void assertCompression(string name, org.apache.hadoop.io.compress.Compressor
					 compressor, org.apache.hadoop.io.compress.Decompressor decompressor, byte[] originalRawData
					)
				{
					int off = 0;
					int len = originalRawData.Length;
					int maxSize = _T1233992766.BLOCK_SIZE - _T1233992766.overheadSpace;
					int compresSize = 0;
					System.Collections.Generic.IList<int> blockLabels = new System.Collections.Generic.List
						<int>();
					java.io.ByteArrayOutputStream compressedOut = new java.io.ByteArrayOutputStream();
					java.io.ByteArrayOutputStream decompressOut = new java.io.ByteArrayOutputStream();
					try
					{
						if (originalRawData.Length > maxSize)
						{
							do
							{
								int bufLen = System.Math.min(len, maxSize);
								compressor.setInput(originalRawData, off, bufLen);
								compressor.finish();
								while (!compressor.finished())
								{
									compresSize = compressor.compress(this.operationBlock, 0, this.operationBlock.Length
										);
									compressedOut.write(this.operationBlock, 0, compresSize);
									blockLabels.add(compresSize);
								}
								compressor.reset();
								off += bufLen;
								len -= bufLen;
							}
							while (len > 0);
						}
						off = 0;
						// compressed bytes
						byte[] compressedBytes = compressedOut.toByteArray();
						foreach (int step in blockLabels)
						{
							decompressor.setInput(compressedBytes, off, step);
							while (!decompressor.finished())
							{
								int dSize = decompressor.decompress(this.operationBlock, 0, this.operationBlock.Length
									);
								decompressOut.write(this.operationBlock, 0, dSize);
							}
							decompressor.reset();
							off = off + step;
						}
						NUnit.Framework.Assert.assertArrayEquals(this.joiner.join(name, "byte arrays not equals error !!!"
							), originalRawData, decompressOut.toByteArray());
					}
					catch (System.Exception ex)
					{
						NUnit.Framework.Assert.Fail(this.joiner.join(name, ex.Message));
					}
					finally
					{
						try
						{
							compressedOut.close();
						}
						catch (System.IO.IOException)
						{
						}
						try
						{
							decompressOut.close();
						}
						catch (System.IO.IOException)
						{
						}
					}
				}
			}

			public static readonly org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
				 COMPRESS_DECOMPRESS_BLOCK = new org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
				(new _TesterCompressionStrategy_384());

			private readonly org.apache.hadoop.io.compress.CompressDecompressTester.TesterCompressionStrategy
				 testerStrategy;

			internal CompressionTestStrategy(org.apache.hadoop.io.compress.CompressDecompressTester.TesterCompressionStrategy
				 testStrategy)
			{
				this.testerStrategy = testStrategy;
			}

			public org.apache.hadoop.io.compress.CompressDecompressTester.TesterCompressionStrategy
				 getTesterStrategy()
			{
				return org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy
					.testerStrategy;
			}
		}

		internal sealed class TesterPair<T, E>
			where T : org.apache.hadoop.io.compress.Compressor
			where E : org.apache.hadoop.io.compress.Decompressor
		{
			private readonly T compressor;

			private readonly E decompressor;

			private readonly string name;

			internal TesterPair(string name, T compressor, E decompressor)
			{
				this.compressor = compressor;
				this.decompressor = decompressor;
				this.name = name;
			}

			public void end()
			{
				org.apache.hadoop.conf.Configuration cfg = new org.apache.hadoop.conf.Configuration
					();
				compressor.reinit(cfg);
				compressor.end();
				decompressor.end();
			}

			public T getCompressor()
			{
				return compressor;
			}

			public E getDecompressor()
			{
				return decompressor;
			}

			public string getName()
			{
				return name;
			}
		}

		/// <summary>Method for compressor availability check</summary>
		private static bool isAvailable<T, E>(org.apache.hadoop.io.compress.CompressDecompressTester.TesterPair
			<T, E> pair)
			where T : org.apache.hadoop.io.compress.Compressor
			where E : org.apache.hadoop.io.compress.Decompressor
		{
			org.apache.hadoop.io.compress.Compressor compressor = pair.compressor;
			if (Sharpen.Runtime.getClassForObject(compressor).isAssignableFrom(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.compress.lz4.Lz4Compressor))) && (org.apache.hadoop.util.NativeCodeLoader
				.isNativeCodeLoaded()))
			{
				return true;
			}
			else
			{
				if (Sharpen.Runtime.getClassForObject(compressor).isAssignableFrom(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater))) && org.apache.hadoop.util.NativeCodeLoader
					.isNativeCodeLoaded())
				{
					return true;
				}
				else
				{
					if (Sharpen.Runtime.getClassForObject(compressor).isAssignableFrom(Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.io.compress.zlib.ZlibCompressor))))
					{
						return org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(new org.apache.hadoop.conf.Configuration
							());
					}
					else
					{
						if (Sharpen.Runtime.getClassForObject(compressor).isAssignableFrom(Sharpen.Runtime.getClassForType
							(typeof(org.apache.hadoop.io.compress.snappy.SnappyCompressor))) && isNativeSnappyLoadable
							())
						{
							return true;
						}
					}
				}
			}
			return false;
		}

		internal abstract class TesterCompressionStrategy
		{
			protected internal readonly org.apache.log4j.Logger logger;

			internal abstract void assertCompression(string name, org.apache.hadoop.io.compress.Compressor
				 compressor, org.apache.hadoop.io.compress.Decompressor decompressor, byte[] originalRawData
				);

			public TesterCompressionStrategy()
			{
				logger = org.apache.log4j.Logger.getLogger(Sharpen.Runtime.getClassForObject(this
					));
			}
		}
	}
}
