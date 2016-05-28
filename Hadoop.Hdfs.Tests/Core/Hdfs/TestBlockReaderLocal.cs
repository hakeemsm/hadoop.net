using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestBlockReaderLocal
	{
		private static TemporarySocketDirectory sockDir;

		[BeforeClass]
		public static void Init()
		{
			sockDir = new TemporarySocketDirectory();
			DomainSocket.DisableBindPathValidation();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Shutdown()
		{
			sockDir.Close();
		}

		public static void AssertArrayRegionsEqual(byte[] buf1, int off1, byte[] buf2, int
			 off2, int len)
		{
			for (int i = 0; i < len; i++)
			{
				if (buf1[off1 + i] != buf2[off2 + i])
				{
					NUnit.Framework.Assert.Fail("arrays differ at byte " + i + ". " + "The first array has "
						 + (int)buf1[off1 + i] + ", but the second array has " + (int)buf2[off2 + i]);
				}
			}
		}

		/// <summary>Similar to IOUtils#readFully().</summary>
		/// <remarks>Similar to IOUtils#readFully(). Reads bytes in a loop.</remarks>
		/// <param name="reader">The BlockReaderLocal to read bytes from</param>
		/// <param name="buf">The ByteBuffer to read into</param>
		/// <param name="off">The offset in the buffer to read into</param>
		/// <param name="len">The number of bytes to read.</param>
		/// <exception cref="System.IO.IOException">If it could not read the requested number of bytes
		/// 	</exception>
		private static void ReadFully(BlockReaderLocal reader, ByteBuffer buf, int off, int
			 len)
		{
			int amt = len;
			while (amt > 0)
			{
				buf.Limit(off + len);
				buf.Position(off);
				long ret = reader.Read(buf);
				if (ret < 0)
				{
					throw new EOFException("Premature EOF from BlockReaderLocal " + "after reading " 
						+ (len - amt) + " byte(s).");
				}
				amt -= ret;
				off += ret;
			}
		}

		private class BlockReaderLocalTest
		{
			internal const int TestLength = 12345;

			internal const int BytesPerChecksum = 512;

			public virtual void SetConfiguration(HdfsConfiguration conf)
			{
			}

			// default: no-op
			/// <exception cref="System.IO.IOException"/>
			public virtual void Setup(FilePath blockFile, bool usingChecksums)
			{
			}

			// default: no-op
			/// <exception cref="System.IO.IOException"/>
			public virtual void DoTest(BlockReaderLocal reader, byte[] original)
			{
			}
			// default: no-op
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RunBlockReaderLocalTest(TestBlockReaderLocal.BlockReaderLocalTest
			 test, bool checksum, long readahead)
		{
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
			MiniDFSCluster cluster = null;
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, !checksum
				);
			conf.SetLong(DFSConfigKeys.DfsBytesPerChecksumKey, TestBlockReaderLocal.BlockReaderLocalTest
				.BytesPerChecksum);
			conf.Set(DFSConfigKeys.DfsChecksumTypeKey, "CRC32C");
			conf.SetLong(DFSConfigKeys.DfsClientCacheReadahead, readahead);
			test.SetConfiguration(conf);
			FileInputStream dataIn = null;
			FileInputStream metaIn = null;
			Path TestPath = new Path("/a");
			long RandomSeed = 4567L;
			BlockReaderLocal blockReaderLocal = null;
			FSDataInputStream fsIn = null;
			byte[] original = new byte[TestBlockReaderLocal.BlockReaderLocalTest.TestLength];
			FileSystem fs = null;
			ShortCircuitShm shm = null;
			RandomAccessFile raf = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestBlockReaderLocal.BlockReaderLocalTest.TestLength
					, (short)1, RandomSeed);
				try
				{
					DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
						 + e);
				}
				catch (TimeoutException e)
				{
					NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
						 + e);
				}
				fsIn = fs.Open(TestPath);
				IOUtils.ReadFully(fsIn, original, 0, TestBlockReaderLocal.BlockReaderLocalTest.TestLength
					);
				fsIn.Close();
				fsIn = null;
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, TestPath);
				FilePath dataFile = cluster.GetBlockFile(0, block);
				FilePath metaFile = cluster.GetBlockMetadataFile(0, block);
				ShortCircuitCache shortCircuitCache = ClientContext.GetFromConf(conf).GetShortCircuitCache
					();
				cluster.Shutdown();
				cluster = null;
				test.Setup(dataFile, checksum);
				FileInputStream[] streams = new FileInputStream[] { new FileInputStream(dataFile)
					, new FileInputStream(metaFile) };
				dataIn = streams[0];
				metaIn = streams[1];
				ExtendedBlockId key = new ExtendedBlockId(block.GetBlockId(), block.GetBlockPoolId
					());
				raf = new RandomAccessFile(new FilePath(sockDir.GetDir().GetAbsolutePath(), UUID.
					RandomUUID().ToString()), "rw");
				raf.SetLength(8192);
				FileInputStream shmStream = new FileInputStream(raf.GetFD());
				shm = new ShortCircuitShm(ShortCircuitShm.ShmId.CreateRandom(), shmStream);
				ShortCircuitReplica replica = new ShortCircuitReplica(key, dataIn, metaIn, shortCircuitCache
					, Time.Now(), shm.AllocAndRegisterSlot(ExtendedBlockId.FromExtendedBlock(block))
					);
				blockReaderLocal = new BlockReaderLocal.Builder(new DFSClient.Conf(conf)).SetFilename
					(TestPath.GetName()).SetBlock(block).SetShortCircuitReplica(replica).SetCachingStrategy
					(new CachingStrategy(false, readahead)).SetVerifyChecksum(checksum).Build();
				dataIn = null;
				metaIn = null;
				test.DoTest(blockReaderLocal, original);
				// BlockReaderLocal should not alter the file position.
				NUnit.Framework.Assert.AreEqual(0, streams[0].GetChannel().Position());
				NUnit.Framework.Assert.AreEqual(0, streams[1].GetChannel().Position());
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				if (dataIn != null)
				{
					dataIn.Close();
				}
				if (metaIn != null)
				{
					metaIn.Close();
				}
				if (blockReaderLocal != null)
				{
					blockReaderLocal.Close();
				}
				if (shm != null)
				{
					shm.Free();
				}
				if (raf != null)
				{
					raf.Close();
				}
			}
		}

		private class TestBlockReaderLocalImmediateClose : TestBlockReaderLocal.BlockReaderLocalTest
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalImmediateClose()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalImmediateClose
				(), true, 0);
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalImmediateClose
				(), false, 0);
		}

		private class TestBlockReaderSimpleReads : TestBlockReaderLocal.BlockReaderLocalTest
		{
			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				byte[] buf = new byte[TestLength];
				reader.ReadFully(buf, 0, 512);
				AssertArrayRegionsEqual(original, 0, buf, 0, 512);
				reader.ReadFully(buf, 512, 512);
				AssertArrayRegionsEqual(original, 512, buf, 512, 512);
				reader.ReadFully(buf, 1024, 513);
				AssertArrayRegionsEqual(original, 1024, buf, 1024, 513);
				reader.ReadFully(buf, 1537, 514);
				AssertArrayRegionsEqual(original, 1537, buf, 1537, 514);
				// Readahead is always at least the size of one chunk in this test.
				NUnit.Framework.Assert.IsTrue(reader.GetMaxReadaheadLength() >= TestBlockReaderLocal.BlockReaderLocalTest
					.BytesPerChecksum);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderSimpleReads()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderSimpleReads(), true
				, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderSimpleReadsShortReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderSimpleReads(), true
				, TestBlockReaderLocal.BlockReaderLocalTest.BytesPerChecksum - 1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderSimpleReadsNoChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderSimpleReads(), false
				, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderSimpleReadsNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderSimpleReads(), true
				, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderSimpleReadsNoChecksumNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderSimpleReads(), false
				, 0);
		}

		private class TestBlockReaderLocalArrayReads2 : TestBlockReaderLocal.BlockReaderLocalTest
		{
			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				byte[] buf = new byte[TestLength];
				reader.ReadFully(buf, 0, 10);
				AssertArrayRegionsEqual(original, 0, buf, 0, 10);
				reader.ReadFully(buf, 10, 100);
				AssertArrayRegionsEqual(original, 10, buf, 10, 100);
				reader.ReadFully(buf, 110, 700);
				AssertArrayRegionsEqual(original, 110, buf, 110, 700);
				reader.ReadFully(buf, 810, 1);
				// from offset 810 to offset 811
				reader.ReadFully(buf, 811, 5);
				AssertArrayRegionsEqual(original, 811, buf, 811, 5);
				reader.ReadFully(buf, 816, 900);
				// skip from offset 816 to offset 1716
				reader.ReadFully(buf, 1716, 5);
				AssertArrayRegionsEqual(original, 1716, buf, 1716, 5);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalArrayReads2()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalArrayReads2(
				), true, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalArrayReads2NoChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalArrayReads2(
				), false, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalArrayReads2NoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalArrayReads2(
				), true, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalArrayReads2NoChecksumNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalArrayReads2(
				), false, 0);
		}

		private class TestBlockReaderLocalByteBufferReads : TestBlockReaderLocal.BlockReaderLocalTest
		{
			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				ByteBuffer buf = ByteBuffer.Wrap(new byte[TestLength]);
				ReadFully(reader, buf, 0, 10);
				AssertArrayRegionsEqual(original, 0, ((byte[])buf.Array()), 0, 10);
				ReadFully(reader, buf, 10, 100);
				AssertArrayRegionsEqual(original, 10, ((byte[])buf.Array()), 10, 100);
				ReadFully(reader, buf, 110, 700);
				AssertArrayRegionsEqual(original, 110, ((byte[])buf.Array()), 110, 700);
				reader.Skip(1);
				// skip from offset 810 to offset 811
				ReadFully(reader, buf, 811, 5);
				AssertArrayRegionsEqual(original, 811, ((byte[])buf.Array()), 811, 5);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalByteBufferReads()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalByteBufferReads
				(), true, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalByteBufferReadsNoChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalByteBufferReads
				(), false, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalByteBufferReadsNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalByteBufferReads
				(), true, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalByteBufferReadsNoChecksumNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalByteBufferReads
				(), false, 0);
		}

		/// <summary>
		/// Test reads that bypass the bounce buffer (because they are aligned
		/// and bigger than the readahead).
		/// </summary>
		private class TestBlockReaderLocalByteBufferFastLaneReads : TestBlockReaderLocal.BlockReaderLocalTest
		{
			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				ByteBuffer buf = ByteBuffer.AllocateDirect(TestLength);
				ReadFully(reader, buf, 0, 5120);
				buf.Flip();
				AssertArrayRegionsEqual(original, 0, DFSTestUtil.AsArray(buf), 0, 5120);
				reader.Skip(1537);
				ReadFully(reader, buf, 0, 1);
				buf.Flip();
				AssertArrayRegionsEqual(original, 6657, DFSTestUtil.AsArray(buf), 0, 1);
				reader.ForceAnchorable();
				ReadFully(reader, buf, 0, 5120);
				buf.Flip();
				AssertArrayRegionsEqual(original, 6658, DFSTestUtil.AsArray(buf), 0, 5120);
				reader.ForceUnanchorable();
				ReadFully(reader, buf, 0, 513);
				buf.Flip();
				AssertArrayRegionsEqual(original, 11778, DFSTestUtil.AsArray(buf), 0, 513);
				reader.Skip(3);
				ReadFully(reader, buf, 0, 50);
				buf.Flip();
				AssertArrayRegionsEqual(original, 12294, DFSTestUtil.AsArray(buf), 0, 50);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalByteBufferFastLaneReads()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalByteBufferFastLaneReads
				(), true, 2 * TestBlockReaderLocal.BlockReaderLocalTest.BytesPerChecksum);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalByteBufferFastLaneReadsNoChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalByteBufferFastLaneReads
				(), false, 2 * TestBlockReaderLocal.BlockReaderLocalTest.BytesPerChecksum);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalByteBufferFastLaneReadsNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalByteBufferFastLaneReads
				(), true, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalByteBufferFastLaneReadsNoChecksumNoReadahead
			()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalByteBufferFastLaneReads
				(), false, 0);
		}

		private class TestBlockReaderLocalReadCorruptStart : TestBlockReaderLocal.BlockReaderLocalTest
		{
			internal bool usingChecksums = false;

			/// <exception cref="System.IO.IOException"/>
			public override void Setup(FilePath blockFile, bool usingChecksums)
			{
				RandomAccessFile bf = null;
				this.usingChecksums = usingChecksums;
				try
				{
					bf = new RandomAccessFile(blockFile, "rw");
					bf.Write(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
				}
				finally
				{
					if (bf != null)
					{
						bf.Close();
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				byte[] buf = new byte[TestLength];
				if (usingChecksums)
				{
					try
					{
						reader.ReadFully(buf, 0, 10);
						NUnit.Framework.Assert.Fail("did not detect corruption");
					}
					catch (IOException)
					{
					}
				}
				else
				{
					// expected
					reader.ReadFully(buf, 0, 10);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadCorruptStart()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadCorruptStart
				(), true, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		private class TestBlockReaderLocalReadCorrupt : TestBlockReaderLocal.BlockReaderLocalTest
		{
			internal bool usingChecksums = false;

			/// <exception cref="System.IO.IOException"/>
			public override void Setup(FilePath blockFile, bool usingChecksums)
			{
				RandomAccessFile bf = null;
				this.usingChecksums = usingChecksums;
				try
				{
					bf = new RandomAccessFile(blockFile, "rw");
					bf.Seek(1539);
					bf.Write(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
				}
				finally
				{
					if (bf != null)
					{
						bf.Close();
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				byte[] buf = new byte[TestLength];
				try
				{
					reader.ReadFully(buf, 0, 10);
					AssertArrayRegionsEqual(original, 0, buf, 0, 10);
					reader.ReadFully(buf, 10, 100);
					AssertArrayRegionsEqual(original, 10, buf, 10, 100);
					reader.ReadFully(buf, 110, 700);
					AssertArrayRegionsEqual(original, 110, buf, 110, 700);
					reader.Skip(1);
					// skip from offset 810 to offset 811
					reader.ReadFully(buf, 811, 5);
					AssertArrayRegionsEqual(original, 811, buf, 811, 5);
					reader.ReadFully(buf, 816, 900);
					if (usingChecksums)
					{
						// We should detect the corruption when using a checksum file.
						NUnit.Framework.Assert.Fail("did not detect corruption");
					}
				}
				catch (ChecksumException)
				{
					if (!usingChecksums)
					{
						NUnit.Framework.Assert.Fail("didn't expect to get ChecksumException: not " + "using checksums."
							);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadCorrupt()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadCorrupt(
				), true, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadCorruptNoChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadCorrupt(
				), false, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadCorruptNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadCorrupt(
				), true, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadCorruptNoChecksumNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadCorrupt(
				), false, 0);
		}

		private class TestBlockReaderLocalWithMlockChanges : TestBlockReaderLocal.BlockReaderLocalTest
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Setup(FilePath blockFile, bool usingChecksums)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				ByteBuffer buf = ByteBuffer.Wrap(new byte[TestLength]);
				reader.Skip(1);
				ReadFully(reader, buf, 1, 9);
				AssertArrayRegionsEqual(original, 1, ((byte[])buf.Array()), 1, 9);
				ReadFully(reader, buf, 10, 100);
				AssertArrayRegionsEqual(original, 10, ((byte[])buf.Array()), 10, 100);
				reader.ForceAnchorable();
				ReadFully(reader, buf, 110, 700);
				AssertArrayRegionsEqual(original, 110, ((byte[])buf.Array()), 110, 700);
				reader.ForceUnanchorable();
				reader.Skip(1);
				// skip from offset 810 to offset 811
				ReadFully(reader, buf, 811, 5);
				AssertArrayRegionsEqual(original, 811, ((byte[])buf.Array()), 811, 5);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalWithMlockChanges()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalWithMlockChanges
				(), true, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalWithMlockChangesNoChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalWithMlockChanges
				(), false, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalWithMlockChangesNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalWithMlockChanges
				(), true, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalWithMlockChangesNoChecksumNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalWithMlockChanges
				(), false, 0);
		}

		private class TestBlockReaderLocalOnFileWithoutChecksum : TestBlockReaderLocal.BlockReaderLocalTest
		{
			public override void SetConfiguration(HdfsConfiguration conf)
			{
				conf.Set(DFSConfigKeys.DfsChecksumTypeKey, "NULL");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				NUnit.Framework.Assert.IsTrue(!reader.GetVerifyChecksum());
				ByteBuffer buf = ByteBuffer.Wrap(new byte[TestLength]);
				reader.Skip(1);
				ReadFully(reader, buf, 1, 9);
				AssertArrayRegionsEqual(original, 1, ((byte[])buf.Array()), 1, 9);
				ReadFully(reader, buf, 10, 100);
				AssertArrayRegionsEqual(original, 10, ((byte[])buf.Array()), 10, 100);
				reader.ForceAnchorable();
				ReadFully(reader, buf, 110, 700);
				AssertArrayRegionsEqual(original, 110, ((byte[])buf.Array()), 110, 700);
				reader.ForceUnanchorable();
				reader.Skip(1);
				// skip from offset 810 to offset 811
				ReadFully(reader, buf, 811, 5);
				AssertArrayRegionsEqual(original, 811, ((byte[])buf.Array()), 811, 5);
			}
		}

		private class TestBlockReaderLocalReadZeroBytes : TestBlockReaderLocal.BlockReaderLocalTest
		{
			/// <exception cref="System.IO.IOException"/>
			public override void DoTest(BlockReaderLocal reader, byte[] original)
			{
				byte[] emptyArr = new byte[0];
				NUnit.Framework.Assert.AreEqual(0, reader.Read(emptyArr, 0, 0));
				ByteBuffer emptyBuf = ByteBuffer.Wrap(emptyArr);
				NUnit.Framework.Assert.AreEqual(0, reader.Read(emptyBuf));
				reader.Skip(1);
				NUnit.Framework.Assert.AreEqual(0, reader.Read(emptyArr, 0, 0));
				NUnit.Framework.Assert.AreEqual(0, reader.Read(emptyBuf));
				reader.Skip(TestBlockReaderLocal.BlockReaderLocalTest.TestLength - 1);
				NUnit.Framework.Assert.AreEqual(-1, reader.Read(emptyArr, 0, 0));
				NUnit.Framework.Assert.AreEqual(-1, reader.Read(emptyBuf));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalOnFileWithoutChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalOnFileWithoutChecksum
				(), true, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalOnFileWithoutChecksumNoChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalOnFileWithoutChecksum
				(), false, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalOnFileWithoutChecksumNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalOnFileWithoutChecksum
				(), true, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalOnFileWithoutChecksumNoChecksumNoReadahead
			()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalOnFileWithoutChecksum
				(), false, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadZeroBytes()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadZeroBytes
				(), true, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadZeroBytesNoChecksum()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadZeroBytes
				(), false, DFSConfigKeys.DfsDatanodeReadaheadBytesDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadZeroBytesNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadZeroBytes
				(), true, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReaderLocalReadZeroBytesNoChecksumNoReadahead()
		{
			RunBlockReaderLocalTest(new TestBlockReaderLocal.TestBlockReaderLocalReadZeroBytes
				(), false, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStatisticsForShortCircuitLocalRead()
		{
			TestStatistics(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStatisticsForLocalRead()
		{
			TestStatistics(false);
		}

		/// <exception cref="System.Exception"/>
		private void TestStatistics(bool isShortCircuit)
		{
			Assume.AssumeTrue(DomainSocket.GetLoadingFailureReason() == null);
			HdfsConfiguration conf = new HdfsConfiguration();
			TemporarySocketDirectory sockDir = null;
			if (isShortCircuit)
			{
				DFSInputStream.tcpReadsDisabledForTesting = true;
				sockDir = new TemporarySocketDirectory();
				conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), "TestStatisticsForLocalRead.%d.sock"
					).GetAbsolutePath());
				conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
				DomainSocket.DisableBindPathValidation();
			}
			else
			{
				conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, false);
			}
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			long RandomSeed = 4567L;
			FSDataInputStream fsIn = null;
			byte[] original = new byte[TestBlockReaderLocal.BlockReaderLocalTest.TestLength];
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestBlockReaderLocal.BlockReaderLocalTest.TestLength
					, (short)1, RandomSeed);
				try
				{
					DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
						 + e);
				}
				catch (TimeoutException e)
				{
					NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
						 + e);
				}
				fsIn = fs.Open(TestPath);
				IOUtils.ReadFully(fsIn, original, 0, TestBlockReaderLocal.BlockReaderLocalTest.TestLength
					);
				HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
				NUnit.Framework.Assert.AreEqual(TestBlockReaderLocal.BlockReaderLocalTest.TestLength
					, dfsIn.GetReadStatistics().GetTotalBytesRead());
				NUnit.Framework.Assert.AreEqual(TestBlockReaderLocal.BlockReaderLocalTest.TestLength
					, dfsIn.GetReadStatistics().GetTotalLocalBytesRead());
				if (isShortCircuit)
				{
					NUnit.Framework.Assert.AreEqual(TestBlockReaderLocal.BlockReaderLocalTest.TestLength
						, dfsIn.GetReadStatistics().GetTotalShortCircuitBytesRead());
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(0, dfsIn.GetReadStatistics().GetTotalShortCircuitBytesRead
						());
				}
				fsIn.Close();
				fsIn = null;
			}
			finally
			{
				DFSInputStream.tcpReadsDisabledForTesting = false;
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				if (sockDir != null)
				{
					sockDir.Close();
				}
			}
		}
	}
}
