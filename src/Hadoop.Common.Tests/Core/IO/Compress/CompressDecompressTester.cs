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
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Compress.Lz4;
using Org.Apache.Hadoop.IO.Compress.Snappy;
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;


namespace Org.Apache.Hadoop.IO.Compress
{
	public class CompressDecompressTester<T, E>
		where T : Compressor
		where E : Decompressor
	{
		private static readonly Logger logger = Logger.GetLogger(typeof(Org.Apache.Hadoop.IO.Compress.CompressDecompressTester
			));

		private readonly byte[] originalRawData;

		private ImmutableList<CompressDecompressTester.TesterPair<T, E>> pairs = ImmutableList
			.Of();

		private ImmutableList.Builder<CompressDecompressTester.TesterPair<T, E>> builder = 
			ImmutableList.Builder();

		private ImmutableSet<CompressDecompressTester.CompressionTestStrategy> stateges = 
			ImmutableSet.Of();

		private CompressDecompressTester.PreAssertionTester<T, E> assertionDelegate;

		public CompressDecompressTester(byte[] originalRawData)
		{
			this.originalRawData = Arrays.CopyOf(originalRawData, originalRawData.Length);
			this.assertionDelegate = new _PreAssertionTester_65();
		}

		private sealed class _PreAssertionTester_65 : CompressDecompressTester.PreAssertionTester
			<T, E>
		{
			public _PreAssertionTester_65()
			{
			}

			public ImmutableList<CompressDecompressTester.TesterPair<T, E>> FilterOnAssumeWhat
				(ImmutableList<CompressDecompressTester.TesterPair<T, E>> pairs)
			{
				ImmutableList.Builder<CompressDecompressTester.TesterPair<T, E>> builder = ImmutableList
					.Builder();
				foreach (CompressDecompressTester.TesterPair<T, E> pair in pairs)
				{
					if (Org.Apache.Hadoop.IO.Compress.CompressDecompressTester.IsAvailable(pair))
					{
						builder.Add(pair);
					}
				}
				return ((ImmutableList<CompressDecompressTester.TesterPair<T, E>>)builder.Build()
					);
			}
		}

		private static bool IsNativeSnappyLoadable()
		{
			bool snappyAvailable = false;
			bool loaded = false;
			try
			{
				Runtime.LoadLibrary("snappy");
				logger.Warn("Snappy native library is available");
				snappyAvailable = true;
				bool hadoopNativeAvailable = NativeCodeLoader.IsNativeCodeLoaded();
				loaded = snappyAvailable && hadoopNativeAvailable;
				if (loaded)
				{
					logger.Info("Snappy native library loaded");
				}
				else
				{
					logger.Warn("Snappy native library not loaded");
				}
			}
			catch (Exception t)
			{
				logger.Warn("Failed to load snappy: ", t);
				return false;
			}
			return loaded;
		}

		public static Org.Apache.Hadoop.IO.Compress.CompressDecompressTester<T, E> Of<T, 
			E>(byte[] rawData)
			where T : Compressor
			where E : Decompressor
		{
			return new Org.Apache.Hadoop.IO.Compress.CompressDecompressTester<T, E>(rawData);
		}

		public virtual Org.Apache.Hadoop.IO.Compress.CompressDecompressTester<T, E> WithCompressDecompressPair
			(T compressor, E decompressor)
		{
			AddPair(compressor, decompressor, Joiner.On("_").Join(compressor.GetType().GetCanonicalName
				(), decompressor.GetType().GetCanonicalName()));
			return this;
		}

		public virtual Org.Apache.Hadoop.IO.Compress.CompressDecompressTester<T, E> WithTestCases
			(ImmutableSet<CompressDecompressTester.CompressionTestStrategy> stateges)
		{
			this.stateges = ImmutableSet.CopyOf(stateges);
			return this;
		}

		private void AddPair(T compressor, E decompressor, string name)
		{
			builder.Add(new CompressDecompressTester.TesterPair<T, E>(name, compressor, decompressor
				));
		}

		/// <exception cref="InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		public virtual void Test()
		{
			pairs = ((ImmutableList<CompressDecompressTester.TesterPair<T, E>>)builder.Build(
				));
			pairs = assertionDelegate.FilterOnAssumeWhat(pairs);
			foreach (CompressDecompressTester.TesterPair<T, E> pair in pairs)
			{
				foreach (CompressDecompressTester.CompressionTestStrategy strategy in stateges)
				{
					strategy.GetTesterStrategy().AssertCompression(pair.GetName(), pair.GetCompressor
						(), pair.GetDecompressor(), Arrays.CopyOf(originalRawData, originalRawData.Length
						));
				}
			}
			EndAll(pairs);
		}

		private void EndAll(ImmutableList<CompressDecompressTester.TesterPair<T, E>> pairs
			)
		{
			foreach (CompressDecompressTester.TesterPair<T, E> pair in pairs)
			{
				pair.End();
			}
		}

		internal interface PreAssertionTester<T, E>
			where T : Compressor
			where E : Decompressor
		{
			ImmutableList<CompressDecompressTester.TesterPair<T, E>> FilterOnAssumeWhat(ImmutableList
				<CompressDecompressTester.TesterPair<T, E>> pairs);
		}

		[System.Serializable]
		public sealed class CompressionTestStrategy
		{
			private sealed class _TesterCompressionStrategy_155 : CompressDecompressTester.TesterCompressionStrategy
			{
				public _TesterCompressionStrategy_155()
				{
					this.joiner = Joiner.On("- ");
				}

				private readonly Joiner joiner;

				internal override void AssertCompression(string name, Compressor compressor, Decompressor
					 decompressor, byte[] rawData)
				{
					Assert.True(this.CheckSetInputNullPointerException(compressor));
					Assert.True(this.CheckSetInputNullPointerException(decompressor
						));
					Assert.True(this.CheckCompressArrayIndexOutOfBoundsException(compressor
						, rawData));
					Assert.True(this.CheckCompressArrayIndexOutOfBoundsException(decompressor
						, rawData));
					Assert.True(this.CheckCompressNullPointerException(compressor, 
						rawData));
					Assert.True(this.CheckCompressNullPointerException(decompressor
						, rawData));
					Assert.True(this.CheckSetInputArrayIndexOutOfBoundsException(compressor
						));
					Assert.True(this.CheckSetInputArrayIndexOutOfBoundsException(decompressor
						));
				}

				private bool CheckSetInputNullPointerException(Compressor compressor)
				{
					try
					{
						compressor.SetInput(null, 0, 1);
					}
					catch (ArgumentNullException)
					{
						return true;
					}
					catch (Exception)
					{
						this.logger.Error(this.joiner.Join(compressor.GetType().GetCanonicalName(), "checkSetInputNullPointerException error !!!"
							));
					}
					return false;
				}

				private bool CheckCompressNullPointerException(Compressor compressor, byte[] rawData
					)
				{
					try
					{
						compressor.SetInput(rawData, 0, rawData.Length);
						compressor.Compress(null, 0, 1);
					}
					catch (ArgumentNullException)
					{
						return true;
					}
					catch (Exception)
					{
						this.logger.Error(this.joiner.Join(compressor.GetType().GetCanonicalName(), "checkCompressNullPointerException error !!!"
							));
					}
					return false;
				}

				private bool CheckCompressNullPointerException(Decompressor decompressor, byte[] 
					rawData)
				{
					try
					{
						decompressor.SetInput(rawData, 0, rawData.Length);
						decompressor.Decompress(null, 0, 1);
					}
					catch (ArgumentNullException)
					{
						return true;
					}
					catch (Exception)
					{
						this.logger.Error(this.joiner.Join(decompressor.GetType().GetCanonicalName(), "checkCompressNullPointerException error !!!"
							));
					}
					return false;
				}

				private bool CheckSetInputNullPointerException(Decompressor decompressor)
				{
					try
					{
						decompressor.SetInput(null, 0, 1);
					}
					catch (ArgumentNullException)
					{
						return true;
					}
					catch (Exception)
					{
						this.logger.Error(this.joiner.Join(decompressor.GetType().GetCanonicalName(), "checkSetInputNullPointerException error !!!"
							));
					}
					return false;
				}

				private bool CheckSetInputArrayIndexOutOfBoundsException(Compressor compressor)
				{
					try
					{
						compressor.SetInput(new byte[] { unchecked((byte)0) }, 0, -1);
					}
					catch (IndexOutOfRangeException)
					{
						return true;
					}
					catch (Exception)
					{
						this.logger.Error(this.joiner.Join(compressor.GetType().GetCanonicalName(), "checkSetInputArrayIndexOutOfBoundsException error !!!"
							));
					}
					return false;
				}

				private bool CheckCompressArrayIndexOutOfBoundsException(Compressor compressor, byte
					[] rawData)
				{
					try
					{
						compressor.SetInput(rawData, 0, rawData.Length);
						compressor.Compress(new byte[rawData.Length], 0, -1);
					}
					catch (IndexOutOfRangeException)
					{
						return true;
					}
					catch (Exception)
					{
						this.logger.Error(this.joiner.Join(compressor.GetType().GetCanonicalName(), "checkCompressArrayIndexOutOfBoundsException error !!!"
							));
					}
					return false;
				}

				private bool CheckCompressArrayIndexOutOfBoundsException(Decompressor decompressor
					, byte[] rawData)
				{
					try
					{
						decompressor.SetInput(rawData, 0, rawData.Length);
						decompressor.Decompress(new byte[rawData.Length], 0, -1);
					}
					catch (IndexOutOfRangeException)
					{
						return true;
					}
					catch (Exception)
					{
						this.logger.Error(this.joiner.Join(decompressor.GetType().GetCanonicalName(), "checkCompressArrayIndexOutOfBoundsException error !!!"
							));
					}
					return false;
				}

				private bool CheckSetInputArrayIndexOutOfBoundsException(Decompressor decompressor
					)
				{
					try
					{
						decompressor.SetInput(new byte[] { unchecked((byte)0) }, 0, -1);
					}
					catch (IndexOutOfRangeException)
					{
						return true;
					}
					catch (Exception)
					{
						this.logger.Error(this.joiner.Join(decompressor.GetType().GetCanonicalName(), "checkNullPointerException error !!!"
							));
					}
					return false;
				}
			}

			public static readonly CompressDecompressTester.CompressionTestStrategy CompressDecompressErrors
				 = new CompressDecompressTester.CompressionTestStrategy(new _TesterCompressionStrategy_155
				());

			private sealed class _TesterCompressionStrategy_285 : CompressDecompressTester.TesterCompressionStrategy
			{
				public _TesterCompressionStrategy_285()
				{
					this.joiner = Joiner.On("- ");
				}

				internal readonly Joiner joiner;

				internal override void AssertCompression(string name, Compressor compressor, Decompressor
					 decompressor, byte[] rawData)
				{
					int cSize = 0;
					int decompressedSize = 0;
					byte[] compressedResult = new byte[rawData.Length];
					byte[] decompressedBytes = new byte[rawData.Length];
					try
					{
						Assert.True(this.joiner.Join(name, "compressor.needsInput before error !!!"
							), compressor.NeedsInput());
						Assert.True(this.joiner.Join(name, "compressor.getBytesWritten before error !!!"
							), compressor.GetBytesWritten() == 0);
						compressor.SetInput(rawData, 0, rawData.Length);
						compressor.Finish();
						while (!compressor.Finished())
						{
							cSize += compressor.Compress(compressedResult, 0, compressedResult.Length);
						}
						compressor.Reset();
						Assert.True(this.joiner.Join(name, "decompressor.needsInput() before error !!!"
							), decompressor.NeedsInput());
						decompressor.SetInput(compressedResult, 0, cSize);
						NUnit.Framework.Assert.IsFalse(this.joiner.Join(name, "decompressor.needsInput() after error !!!"
							), decompressor.NeedsInput());
						while (!decompressor.Finished())
						{
							decompressedSize = decompressor.Decompress(decompressedBytes, 0, decompressedBytes
								.Length);
						}
						decompressor.Reset();
						Assert.True(this.joiner.Join(name, " byte size not equals error !!!"
							), decompressedSize == rawData.Length);
						Assert.AssertArrayEquals(this.joiner.Join(name, " byte arrays not equals error !!!"
							), rawData, decompressedBytes);
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(this.joiner.Join(name, ex.Message));
					}
				}
			}

			public static readonly CompressDecompressTester.CompressionTestStrategy CompressDecompressSingleBlock
				 = new CompressDecompressTester.CompressionTestStrategy(new _TesterCompressionStrategy_285
				());

			private sealed class _TesterCompressionStrategy_334 : CompressDecompressTester.TesterCompressionStrategy
			{
				public _TesterCompressionStrategy_334()
				{
					this.joiner = Joiner.On("- ");
					this.emptySize = ImmutableMap.Of(typeof(Lz4Compressor), 4, typeof(ZlibCompressor)
						, 16, typeof(SnappyCompressor), 4, typeof(BuiltInZlibDeflater), 16);
				}

				internal readonly Joiner joiner;

				internal readonly ImmutableMap<Type, int> emptySize;

				internal override void AssertCompression(string name, Compressor compressor, Decompressor
					 decompressor, byte[] originalRawData)
				{
					byte[] buf = null;
					ByteArrayInputStream bytesIn = null;
					BlockDecompressorStream blockDecompressorStream = null;
					ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
					// close without write
					try
					{
						compressor.Reset();
						// decompressor.end();
						BlockCompressorStream blockCompressorStream = new BlockCompressorStream(bytesOut, 
							compressor, 1024, 0);
						blockCompressorStream.Close();
						// check compressed output
						buf = bytesOut.ToByteArray();
						int emSize = this.emptySize[compressor.GetType()];
						Assert.Equal(this.joiner.Join(name, "empty stream compressed output size != "
							 + emSize), emSize, buf.Length);
						// use compressed output as input for decompression
						bytesIn = new ByteArrayInputStream(buf);
						// create decompression stream
						blockDecompressorStream = new BlockDecompressorStream(bytesIn, decompressor, 1024
							);
						// no byte is available because stream was closed
						Assert.Equal(this.joiner.Join(name, " return value is not -1")
							, -1, blockDecompressorStream.Read());
					}
					catch (IOException e)
					{
						NUnit.Framework.Assert.Fail(this.joiner.Join(name, e.Message));
					}
					finally
					{
						if (blockDecompressorStream != null)
						{
							try
							{
								bytesOut.Close();
								blockDecompressorStream.Close();
								bytesIn.Close();
								blockDecompressorStream.Close();
							}
							catch (IOException)
							{
							}
						}
					}
				}
			}

			public static readonly CompressDecompressTester.CompressionTestStrategy CompressDecompressWithEmptyStream
				 = new CompressDecompressTester.CompressionTestStrategy(new _TesterCompressionStrategy_334
				());

			private sealed class _TesterCompressionStrategy_384 : CompressDecompressTester.TesterCompressionStrategy
			{
				public _TesterCompressionStrategy_384()
				{
					this.joiner = Joiner.On("- ");
					this.BlockSize = 512;
					this.operationBlock = new byte[_T1233992766.BlockSize];
					this.overheadSpace = _T1233992766.BlockSize / 100 + 12;
				}

				private readonly Joiner joiner;

				private const int BlockSize;

				private readonly byte[] operationBlock;

				private const int overheadSpace;

				// Use default of 512 as bufferSize and compressionOverhead of
				// (1% of bufferSize + 12 bytes) = 18 bytes (zlib algorithm).
				internal override void AssertCompression(string name, Compressor compressor, Decompressor
					 decompressor, byte[] originalRawData)
				{
					int off = 0;
					int len = originalRawData.Length;
					int maxSize = _T1233992766.BlockSize - _T1233992766.overheadSpace;
					int compresSize = 0;
					IList<int> blockLabels = new AList<int>();
					ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
					ByteArrayOutputStream decompressOut = new ByteArrayOutputStream();
					try
					{
						if (originalRawData.Length > maxSize)
						{
							do
							{
								int bufLen = Math.Min(len, maxSize);
								compressor.SetInput(originalRawData, off, bufLen);
								compressor.Finish();
								while (!compressor.Finished())
								{
									compresSize = compressor.Compress(this.operationBlock, 0, this.operationBlock.Length
										);
									compressedOut.Write(this.operationBlock, 0, compresSize);
									blockLabels.AddItem(compresSize);
								}
								compressor.Reset();
								off += bufLen;
								len -= bufLen;
							}
							while (len > 0);
						}
						off = 0;
						// compressed bytes
						byte[] compressedBytes = compressedOut.ToByteArray();
						foreach (int step in blockLabels)
						{
							decompressor.SetInput(compressedBytes, off, step);
							while (!decompressor.Finished())
							{
								int dSize = decompressor.Decompress(this.operationBlock, 0, this.operationBlock.Length
									);
								decompressOut.Write(this.operationBlock, 0, dSize);
							}
							decompressor.Reset();
							off = off + step;
						}
						Assert.AssertArrayEquals(this.joiner.Join(name, "byte arrays not equals error !!!"
							), originalRawData, decompressOut.ToByteArray());
					}
					catch (Exception ex)
					{
						NUnit.Framework.Assert.Fail(this.joiner.Join(name, ex.Message));
					}
					finally
					{
						try
						{
							compressedOut.Close();
						}
						catch (IOException)
						{
						}
						try
						{
							decompressOut.Close();
						}
						catch (IOException)
						{
						}
					}
				}
			}

			public static readonly CompressDecompressTester.CompressionTestStrategy CompressDecompressBlock
				 = new CompressDecompressTester.CompressionTestStrategy(new _TesterCompressionStrategy_384
				());

			private readonly CompressDecompressTester.TesterCompressionStrategy testerStrategy;

			internal CompressionTestStrategy(CompressDecompressTester.TesterCompressionStrategy
				 testStrategy)
			{
				this.testerStrategy = testStrategy;
			}

			public CompressDecompressTester.TesterCompressionStrategy GetTesterStrategy()
			{
				return CompressDecompressTester.CompressionTestStrategy.testerStrategy;
			}
		}

		internal sealed class TesterPair<T, E>
			where T : Compressor
			where E : Decompressor
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

			public void End()
			{
				Configuration cfg = new Configuration();
				compressor.Reinit(cfg);
				compressor.End();
				decompressor.End();
			}

			public T GetCompressor()
			{
				return compressor;
			}

			public E GetDecompressor()
			{
				return decompressor;
			}

			public string GetName()
			{
				return name;
			}
		}

		/// <summary>Method for compressor availability check</summary>
		private static bool IsAvailable<T, E>(CompressDecompressTester.TesterPair<T, E> pair
			)
			where T : Compressor
			where E : Decompressor
		{
			Compressor compressor = pair.compressor;
			if (compressor.GetType().IsAssignableFrom(typeof(Lz4Compressor)) && (NativeCodeLoader
				.IsNativeCodeLoaded()))
			{
				return true;
			}
			else
			{
				if (compressor.GetType().IsAssignableFrom(typeof(BuiltInZlibDeflater)) && NativeCodeLoader
					.IsNativeCodeLoaded())
				{
					return true;
				}
				else
				{
					if (compressor.GetType().IsAssignableFrom(typeof(ZlibCompressor)))
					{
						return ZlibFactory.IsNativeZlibLoaded(new Configuration());
					}
					else
					{
						if (compressor.GetType().IsAssignableFrom(typeof(SnappyCompressor)) && IsNativeSnappyLoadable
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
			protected internal readonly Logger logger = Logger.GetLogger(GetType());

			internal abstract void AssertCompression(string name, Compressor compressor, Decompressor
				 decompressor, byte[] originalRawData);
		}
	}
}
