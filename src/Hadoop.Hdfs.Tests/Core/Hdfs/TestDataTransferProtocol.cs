using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This tests data transfer protocol handling in the Datanode.</summary>
	/// <remarks>
	/// This tests data transfer protocol handling in the Datanode. It sends
	/// various forms of wrong data and verifies that Datanode handles it well.
	/// </remarks>
	public class TestDataTransferProtocol
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestDataTransferProtocol"
			);

		private static readonly DataChecksum DefaultChecksum = DataChecksum.NewDataChecksum
			(DataChecksum.Type.Crc32c, 512);

		internal DatanodeID datanode;

		internal IPEndPoint dnAddr;

		internal readonly ByteArrayOutputStream sendBuf = new ByteArrayOutputStream(128);

		internal readonly DataOutputStream sendOut;

		internal readonly Sender sender;

		internal readonly ByteArrayOutputStream recvBuf = new ByteArrayOutputStream(128);

		internal readonly DataOutputStream recvOut;

		/// <exception cref="System.IO.IOException"/>
		private void SendRecvData(string testDescription, bool eofExpected)
		{
			/* Opens a socket to datanode
			* sends the data in sendBuf.
			* If there is data in expectedBuf, expects to receive the data
			*     from datanode that matches expectedBuf.
			* If there is an exception while recieving, throws it
			*     only if exceptionExcepted is false.
			*/
			Socket sock = null;
			try
			{
				if (testDescription != null)
				{
					Log.Info("Testing : " + testDescription);
				}
				Log.Info("Going to write:" + StringUtils.ByteToHexString(sendBuf.ToByteArray()));
				sock = new Socket();
				sock.Connect(dnAddr, HdfsServerConstants.ReadTimeout);
				sock.ReceiveTimeout = HdfsServerConstants.ReadTimeout;
				OutputStream @out = sock.GetOutputStream();
				// Should we excuse 
				byte[] retBuf = new byte[recvBuf.Size()];
				DataInputStream @in = new DataInputStream(sock.GetInputStream());
				@out.Write(sendBuf.ToByteArray());
				@out.Flush();
				try
				{
					@in.ReadFully(retBuf);
				}
				catch (EOFException eof)
				{
					if (eofExpected)
					{
						Log.Info("Got EOF as expected.");
						return;
					}
					throw;
				}
				string received = StringUtils.ByteToHexString(retBuf);
				string expected = StringUtils.ByteToHexString(recvBuf.ToByteArray());
				Log.Info("Received: " + received);
				Log.Info("Expected: " + expected);
				if (eofExpected)
				{
					throw new IOException("Did not recieve IOException when an exception " + "is expected while reading from "
						 + datanode);
				}
				NUnit.Framework.Assert.AreEqual(expected, received);
			}
			finally
			{
				IOUtils.CloseSocket(sock);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateFile(FileSystem fs, Path path, int fileLen)
		{
			byte[] arr = new byte[fileLen];
			FSDataOutputStream @out = fs.Create(path);
			@out.Write(arr);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReadFile(FileSystem fs, Path path, int fileLen)
		{
			byte[] arr = new byte[fileLen];
			FSDataInputStream @in = fs.Open(path);
			@in.ReadFully(arr);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteZeroLengthPacket(ExtendedBlock block, string description)
		{
			PacketHeader hdr = new PacketHeader(8, block.GetNumBytes(), 100, true, 0, false);
			// size of packet
			// OffsetInBlock
			// sequencenumber
			// lastPacketInBlock
			// chunk length
			// sync block
			hdr.Write(sendOut);
			sendOut.WriteInt(0);
			// zero checksum
			//ok finally write a block with 0 len
			SendResponse(DataTransferProtos.Status.Success, string.Empty, null, recvOut);
			new PipelineAck(100, new int[] { PipelineAck.CombineHeader(PipelineAck.ECN.Disabled
				, DataTransferProtos.Status.Success) }).Write(recvOut);
			SendRecvData(description, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendResponse(DataTransferProtos.Status status, string firstBadLink, 
			string message, DataOutputStream @out)
		{
			DataTransferProtos.BlockOpResponseProto.Builder builder = DataTransferProtos.BlockOpResponseProto
				.NewBuilder().SetStatus(status);
			if (firstBadLink != null)
			{
				builder.SetFirstBadLink(firstBadLink);
			}
			if (message != null)
			{
				builder.SetMessage(message);
			}
			((DataTransferProtos.BlockOpResponseProto)builder.Build()).WriteDelimitedTo(@out);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestWrite(ExtendedBlock block, BlockConstructionStage stage, long newGS
			, string description, bool eofExcepted)
		{
			sendBuf.Reset();
			recvBuf.Reset();
			WriteBlock(block, stage, newGS, DefaultChecksum);
			if (eofExcepted)
			{
				SendResponse(DataTransferProtos.Status.Error, null, null, recvOut);
				SendRecvData(description, true);
			}
			else
			{
				if (stage == BlockConstructionStage.PipelineCloseRecovery)
				{
					//ok finally write a block with 0 len
					SendResponse(DataTransferProtos.Status.Success, string.Empty, null, recvOut);
					SendRecvData(description, false);
				}
				else
				{
					WriteZeroLengthPacket(block, description);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOpWrite()
		{
			int numDataNodes = 1;
			long BlockIdFudge = 128;
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes
				).Build();
			try
			{
				cluster.WaitActive();
				string poolId = cluster.GetNamesystem().GetBlockPoolId();
				datanode = DataNodeTestUtils.GetDNRegistrationForBP(cluster.GetDataNodes()[0], poolId
					);
				dnAddr = NetUtils.CreateSocketAddr(datanode.GetXferAddr());
				FileSystem fileSys = cluster.GetFileSystem();
				/* Test writing to finalized replicas */
				Path file = new Path("dataprotocol.dat");
				DFSTestUtil.CreateFile(fileSys, file, 1L, (short)numDataNodes, 0L);
				// get the first blockid for the file
				ExtendedBlock firstBlock = DFSTestUtil.GetFirstBlock(fileSys, file);
				// test PIPELINE_SETUP_CREATE on a finalized block
				TestWrite(firstBlock, BlockConstructionStage.PipelineSetupCreate, 0L, "Cannot create an existing block"
					, true);
				// test PIPELINE_DATA_STREAMING on a finalized block
				TestWrite(firstBlock, BlockConstructionStage.DataStreaming, 0L, "Unexpected stage"
					, true);
				// test PIPELINE_SETUP_STREAMING_RECOVERY on an existing block
				long newGS = firstBlock.GetGenerationStamp() + 1;
				TestWrite(firstBlock, BlockConstructionStage.PipelineSetupStreamingRecovery, newGS
					, "Cannot recover data streaming to a finalized replica", true);
				// test PIPELINE_SETUP_APPEND on an existing block
				newGS = firstBlock.GetGenerationStamp() + 1;
				TestWrite(firstBlock, BlockConstructionStage.PipelineSetupAppend, newGS, "Append to a finalized replica"
					, false);
				firstBlock.SetGenerationStamp(newGS);
				// test PIPELINE_SETUP_APPEND_RECOVERY on an existing block
				file = new Path("dataprotocol1.dat");
				DFSTestUtil.CreateFile(fileSys, file, 1L, (short)numDataNodes, 0L);
				firstBlock = DFSTestUtil.GetFirstBlock(fileSys, file);
				newGS = firstBlock.GetGenerationStamp() + 1;
				TestWrite(firstBlock, BlockConstructionStage.PipelineSetupAppendRecovery, newGS, 
					"Recover appending to a finalized replica", false);
				// test PIPELINE_CLOSE_RECOVERY on an existing block
				file = new Path("dataprotocol2.dat");
				DFSTestUtil.CreateFile(fileSys, file, 1L, (short)numDataNodes, 0L);
				firstBlock = DFSTestUtil.GetFirstBlock(fileSys, file);
				newGS = firstBlock.GetGenerationStamp() + 1;
				TestWrite(firstBlock, BlockConstructionStage.PipelineCloseRecovery, newGS, "Recover failed close to a finalized replica"
					, false);
				firstBlock.SetGenerationStamp(newGS);
				// Test writing to a new block. Don't choose the next sequential
				// block ID to avoid conflicting with IDs chosen by the NN.
				long newBlockId = firstBlock.GetBlockId() + BlockIdFudge;
				ExtendedBlock newBlock = new ExtendedBlock(firstBlock.GetBlockPoolId(), newBlockId
					, 0, firstBlock.GetGenerationStamp());
				// test PIPELINE_SETUP_CREATE on a new block
				TestWrite(newBlock, BlockConstructionStage.PipelineSetupCreate, 0L, "Create a new block"
					, false);
				// test PIPELINE_SETUP_STREAMING_RECOVERY on a new block
				newGS = newBlock.GetGenerationStamp() + 1;
				newBlock.SetBlockId(newBlock.GetBlockId() + 1);
				TestWrite(newBlock, BlockConstructionStage.PipelineSetupStreamingRecovery, newGS, 
					"Recover a new block", true);
				// test PIPELINE_SETUP_APPEND on a new block
				newGS = newBlock.GetGenerationStamp() + 1;
				TestWrite(newBlock, BlockConstructionStage.PipelineSetupAppend, newGS, "Cannot append to a new block"
					, true);
				// test PIPELINE_SETUP_APPEND_RECOVERY on a new block
				newBlock.SetBlockId(newBlock.GetBlockId() + 1);
				newGS = newBlock.GetGenerationStamp() + 1;
				TestWrite(newBlock, BlockConstructionStage.PipelineSetupAppendRecovery, newGS, "Cannot append to a new block"
					, true);
				/* Test writing to RBW replicas */
				Path file1 = new Path("dataprotocol1.dat");
				DFSTestUtil.CreateFile(fileSys, file1, 1L, (short)numDataNodes, 0L);
				DFSOutputStream @out = (DFSOutputStream)(fileSys.Append(file1).GetWrappedStream()
					);
				@out.Write(1);
				@out.Hflush();
				FSDataInputStream @in = fileSys.Open(file1);
				firstBlock = DFSTestUtil.GetAllBlocks(@in)[0].GetBlock();
				firstBlock.SetNumBytes(2L);
				try
				{
					// test PIPELINE_SETUP_CREATE on a RBW block
					TestWrite(firstBlock, BlockConstructionStage.PipelineSetupCreate, 0L, "Cannot create a RBW block"
						, true);
					// test PIPELINE_SETUP_APPEND on an existing block
					newGS = firstBlock.GetGenerationStamp() + 1;
					TestWrite(firstBlock, BlockConstructionStage.PipelineSetupAppend, newGS, "Cannot append to a RBW replica"
						, true);
					// test PIPELINE_SETUP_APPEND on an existing block
					TestWrite(firstBlock, BlockConstructionStage.PipelineSetupAppendRecovery, newGS, 
						"Recover append to a RBW replica", false);
					firstBlock.SetGenerationStamp(newGS);
					// test PIPELINE_SETUP_STREAMING_RECOVERY on a RBW block
					file = new Path("dataprotocol2.dat");
					DFSTestUtil.CreateFile(fileSys, file, 1L, (short)numDataNodes, 0L);
					@out = (DFSOutputStream)(fileSys.Append(file).GetWrappedStream());
					@out.Write(1);
					@out.Hflush();
					@in = fileSys.Open(file);
					firstBlock = DFSTestUtil.GetAllBlocks(@in)[0].GetBlock();
					firstBlock.SetNumBytes(2L);
					newGS = firstBlock.GetGenerationStamp() + 1;
					TestWrite(firstBlock, BlockConstructionStage.PipelineSetupStreamingRecovery, newGS
						, "Recover a RBW replica", false);
				}
				finally
				{
					IOUtils.CloseStream(@in);
					IOUtils.CloseStream(@out);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDataTransferProtocol()
		{
			Random random = new Random();
			int oneMil = 1024 * 1024;
			Path file = new Path("dataprotocol.dat");
			int numDataNodes = 1;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, numDataNodes);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes
				).Build();
			try
			{
				cluster.WaitActive();
				datanode = cluster.GetFileSystem().GetDataNodeStats(HdfsConstants.DatanodeReportType
					.Live)[0];
				dnAddr = NetUtils.CreateSocketAddr(datanode.GetXferAddr());
				FileSystem fileSys = cluster.GetFileSystem();
				int fileLen = Math.Min(conf.GetInt(DFSConfigKeys.DfsBlockSizeKey, 4096), 4096);
				CreateFile(fileSys, file, fileLen);
				// get the first blockid for the file
				ExtendedBlock firstBlock = DFSTestUtil.GetFirstBlock(fileSys, file);
				string poolId = firstBlock.GetBlockPoolId();
				long newBlockId = firstBlock.GetBlockId() + 1;
				recvBuf.Reset();
				sendBuf.Reset();
				// bad version
				recvOut.WriteShort((short)(DataTransferProtocol.DataTransferVersion - 1));
				sendOut.WriteShort((short)(DataTransferProtocol.DataTransferVersion - 1));
				SendRecvData("Wrong Version", true);
				// bad ops
				sendBuf.Reset();
				sendOut.WriteShort((short)DataTransferProtocol.DataTransferVersion);
				sendOut.WriteByte(OP.WriteBlock.code - 1);
				SendRecvData("Wrong Op Code", true);
				/* Test OP_WRITE_BLOCK */
				sendBuf.Reset();
				DataChecksum badChecksum = Org.Mockito.Mockito.Spy(DefaultChecksum);
				Org.Mockito.Mockito.DoReturn(-1).When(badChecksum).GetBytesPerChecksum();
				WriteBlock(poolId, newBlockId, badChecksum);
				recvBuf.Reset();
				SendResponse(DataTransferProtos.Status.Error, null, null, recvOut);
				SendRecvData("wrong bytesPerChecksum while writing", true);
				sendBuf.Reset();
				recvBuf.Reset();
				WriteBlock(poolId, ++newBlockId, DefaultChecksum);
				PacketHeader hdr = new PacketHeader(4, 0, 100, false, -1 - random.Next(oneMil), false
					);
				// size of packet
				// offset in block,
				// seqno
				// last packet
				// bad datalen
				hdr.Write(sendOut);
				SendResponse(DataTransferProtos.Status.Success, string.Empty, null, recvOut);
				new PipelineAck(100, new int[] { PipelineAck.CombineHeader(PipelineAck.ECN.Disabled
					, DataTransferProtos.Status.Error) }).Write(recvOut);
				SendRecvData("negative DATA_CHUNK len while writing block " + newBlockId, true);
				// test for writing a valid zero size block
				sendBuf.Reset();
				recvBuf.Reset();
				WriteBlock(poolId, ++newBlockId, DefaultChecksum);
				hdr = new PacketHeader(8, 0, 100, true, 0, false);
				// size of packet
				// OffsetInBlock
				// sequencenumber
				// lastPacketInBlock
				// chunk length
				hdr.Write(sendOut);
				sendOut.WriteInt(0);
				// zero checksum
				sendOut.Flush();
				//ok finally write a block with 0 len
				SendResponse(DataTransferProtos.Status.Success, string.Empty, null, recvOut);
				new PipelineAck(100, new int[] { PipelineAck.CombineHeader(PipelineAck.ECN.Disabled
					, DataTransferProtos.Status.Success) }).Write(recvOut);
				SendRecvData("Writing a zero len block blockid " + newBlockId, false);
				/* Test OP_READ_BLOCK */
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				ExtendedBlock blk = new ExtendedBlock(bpid, firstBlock.GetLocalBlock());
				long blkid = blk.GetBlockId();
				// bad block id
				sendBuf.Reset();
				recvBuf.Reset();
				blk.SetBlockId(blkid - 1);
				sender.ReadBlock(blk, BlockTokenSecretManager.DummyToken, "cl", 0L, fileLen, true
					, CachingStrategy.NewDefaultStrategy());
				SendRecvData("Wrong block ID " + newBlockId + " for read", false);
				// negative block start offset -1L
				sendBuf.Reset();
				blk.SetBlockId(blkid);
				sender.ReadBlock(blk, BlockTokenSecretManager.DummyToken, "cl", -1L, fileLen, true
					, CachingStrategy.NewDefaultStrategy());
				SendRecvData("Negative start-offset for read for block " + firstBlock.GetBlockId(
					), false);
				// bad block start offset
				sendBuf.Reset();
				sender.ReadBlock(blk, BlockTokenSecretManager.DummyToken, "cl", fileLen, fileLen, 
					true, CachingStrategy.NewDefaultStrategy());
				SendRecvData("Wrong start-offset for reading block " + firstBlock.GetBlockId(), false
					);
				// negative length is ok. Datanode assumes we want to read the whole block.
				recvBuf.Reset();
				((DataTransferProtos.BlockOpResponseProto)DataTransferProtos.BlockOpResponseProto
					.NewBuilder().SetStatus(DataTransferProtos.Status.Success).SetReadOpChecksumInfo
					(DataTransferProtos.ReadOpChecksumInfoProto.NewBuilder().SetChecksum(DataTransferProtoUtil
					.ToProto(DefaultChecksum)).SetChunkOffset(0L)).Build()).WriteDelimitedTo(recvOut
					);
				sendBuf.Reset();
				sender.ReadBlock(blk, BlockTokenSecretManager.DummyToken, "cl", 0L, -1L - random.
					Next(oneMil), true, CachingStrategy.NewDefaultStrategy());
				SendRecvData("Negative length for reading block " + firstBlock.GetBlockId(), false
					);
				// length is more than size of block.
				recvBuf.Reset();
				SendResponse(DataTransferProtos.Status.Error, null, "opReadBlock " + firstBlock +
					 " received exception java.io.IOException:  " + "Offset 0 and length 4097 don't match block "
					 + firstBlock + " ( blockLen 4096 )", recvOut);
				sendBuf.Reset();
				sender.ReadBlock(blk, BlockTokenSecretManager.DummyToken, "cl", 0L, fileLen + 1, 
					true, CachingStrategy.NewDefaultStrategy());
				SendRecvData("Wrong length for reading block " + firstBlock.GetBlockId(), false);
				//At the end of all this, read the file to make sure that succeeds finally.
				sendBuf.Reset();
				sender.ReadBlock(blk, BlockTokenSecretManager.DummyToken, "cl", 0L, fileLen, true
					, CachingStrategy.NewDefaultStrategy());
				ReadFile(fileSys, file, fileLen);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPacketHeader()
		{
			PacketHeader hdr = new PacketHeader(4, 1024, 100, false, 4096, false);
			// size of packet
			// OffsetInBlock
			// sequencenumber
			// lastPacketInBlock
			// chunk length
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			hdr.Write(new DataOutputStream(baos));
			// Read back using DataInput
			PacketHeader readBack = new PacketHeader();
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.ToByteArray());
			readBack.ReadFields(new DataInputStream(bais));
			NUnit.Framework.Assert.AreEqual(hdr, readBack);
			// Read back using ByteBuffer
			readBack = new PacketHeader();
			readBack.ReadFields(ByteBuffer.Wrap(baos.ToByteArray()));
			NUnit.Framework.Assert.AreEqual(hdr, readBack);
			NUnit.Framework.Assert.IsTrue(hdr.SanityCheck(99));
			NUnit.Framework.Assert.IsFalse(hdr.SanityCheck(100));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPipeLineAckCompatibility()
		{
			DataTransferProtos.PipelineAckProto proto = ((DataTransferProtos.PipelineAckProto
				)DataTransferProtos.PipelineAckProto.NewBuilder().SetSeqno(0).AddReply(DataTransferProtos.Status
				.ChecksumOk).Build());
			DataTransferProtos.PipelineAckProto newProto = ((DataTransferProtos.PipelineAckProto
				)DataTransferProtos.PipelineAckProto.NewBuilder().MergeFrom(proto).AddFlag(PipelineAck
				.CombineHeader(PipelineAck.ECN.Supported, DataTransferProtos.Status.ChecksumOk))
				.Build());
			ByteArrayOutputStream oldAckBytes = new ByteArrayOutputStream();
			proto.WriteDelimitedTo(oldAckBytes);
			PipelineAck oldAck = new PipelineAck();
			oldAck.ReadFields(new ByteArrayInputStream(oldAckBytes.ToByteArray()));
			NUnit.Framework.Assert.AreEqual(PipelineAck.CombineHeader(PipelineAck.ECN.Disabled
				, DataTransferProtos.Status.ChecksumOk), oldAck.GetHeaderFlag(0));
			PipelineAck newAck = new PipelineAck();
			ByteArrayOutputStream newAckBytes = new ByteArrayOutputStream();
			newProto.WriteDelimitedTo(newAckBytes);
			newAck.ReadFields(new ByteArrayInputStream(newAckBytes.ToByteArray()));
			NUnit.Framework.Assert.AreEqual(PipelineAck.CombineHeader(PipelineAck.ECN.Supported
				, DataTransferProtos.Status.ChecksumOk), newAck.GetHeaderFlag(0));
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void WriteBlock(string poolId, long blockId, DataChecksum checksum
			)
		{
			WriteBlock(new ExtendedBlock(poolId, blockId), BlockConstructionStage.PipelineSetupCreate
				, 0L, checksum);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void WriteBlock(ExtendedBlock block, BlockConstructionStage stage
			, long newGS, DataChecksum checksum)
		{
			sender.WriteBlock(block, StorageType.Default, BlockTokenSecretManager.DummyToken, 
				"cl", new DatanodeInfo[1], new StorageType[1], null, stage, 0, block.GetNumBytes
				(), block.GetNumBytes(), newGS, checksum, CachingStrategy.NewDefaultStrategy(), 
				false, false, null);
		}

		public TestDataTransferProtocol()
		{
			sendOut = new DataOutputStream(sendBuf);
			sender = new Sender(sendOut);
			recvOut = new DataOutputStream(recvBuf);
		}
	}
}
