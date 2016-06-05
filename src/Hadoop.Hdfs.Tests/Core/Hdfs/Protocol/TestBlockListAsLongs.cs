using System.Collections.Generic;
using Com.Google.Protobuf;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	public class TestBlockListAsLongs
	{
		internal static Block b1 = new Block(1, 11, 111);

		internal static Block b2 = new Block(2, 22, 222);

		internal static Block b3 = new Block(3, 33, 333);

		internal static Block b4 = new Block(4, 44, 444);

		[NUnit.Framework.Test]
		public virtual void TestEmptyReport()
		{
			BlockListAsLongs blocks = CheckReport();
			Assert.AssertArrayEquals(new long[] { 0, 0, -1, -1, -1 }, blocks.GetBlockListAsLongs
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestFinalized()
		{
			BlockListAsLongs blocks = CheckReport(new FinalizedReplica(b1, null, null));
			Assert.AssertArrayEquals(new long[] { 1, 0, 1, 11, 111, -1, -1, -1 }, blocks.GetBlockListAsLongs
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestUc()
		{
			BlockListAsLongs blocks = CheckReport(new ReplicaBeingWritten(b1, null, null, null
				));
			Assert.AssertArrayEquals(new long[] { 0, 1, -1, -1, -1, 1, 11, 111, HdfsServerConstants.ReplicaState
				.Rbw.GetValue() }, blocks.GetBlockListAsLongs());
		}

		[NUnit.Framework.Test]
		public virtual void TestMix()
		{
			BlockListAsLongs blocks = CheckReport(new FinalizedReplica(b1, null, null), new FinalizedReplica
				(b2, null, null), new ReplicaBeingWritten(b3, null, null, null), new ReplicaWaitingToBeRecovered
				(b4, null, null));
			Assert.AssertArrayEquals(new long[] { 2, 2, 1, 11, 111, 2, 22, 222, -1, -1, -1, 3
				, 33, 333, HdfsServerConstants.ReplicaState.Rbw.GetValue(), 4, 44, 444, HdfsServerConstants.ReplicaState
				.Rwr.GetValue() }, blocks.GetBlockListAsLongs());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFuzz()
		{
			Replica[] replicas = new Replica[100000];
			Random rand = new Random(0);
			for (int i = 0; i < replicas.Length; i++)
			{
				Block b = new Block(rand.NextLong(), i, i << 4);
				switch (rand.Next(2))
				{
					case 0:
					{
						replicas[i] = new FinalizedReplica(b, null, null);
						break;
					}

					case 1:
					{
						replicas[i] = new ReplicaBeingWritten(b, null, null, null);
						break;
					}

					case 2:
					{
						replicas[i] = new ReplicaWaitingToBeRecovered(b, null, null);
						break;
					}
				}
			}
			CheckReport(replicas);
		}

		private BlockListAsLongs CheckReport(params Replica[] replicas)
		{
			IDictionary<long, Replica> expectedReplicas = new Dictionary<long, Replica>();
			foreach (Replica replica in replicas)
			{
				expectedReplicas[replica.GetBlockId()] = replica;
			}
			expectedReplicas = Sharpen.Collections.UnmodifiableMap(expectedReplicas);
			// encode the blocks and extract the buffers
			BlockListAsLongs blocks = BlockListAsLongs.Encode(expectedReplicas.Values);
			IList<ByteString> buffers = blocks.GetBlocksBuffers();
			// convert to old-style list of longs
			IList<long> longs = new AList<long>();
			foreach (long value in blocks.GetBlockListAsLongs())
			{
				longs.AddItem(value);
			}
			// decode the buffers and verify its contents
			BlockListAsLongs decodedBlocks = BlockListAsLongs.DecodeBuffers(expectedReplicas.
				Count, buffers);
			CheckReplicas(expectedReplicas, decodedBlocks);
			// decode the long and verify its contents
			BlockListAsLongs decodedList = BlockListAsLongs.DecodeLongs(longs);
			CheckReplicas(expectedReplicas, decodedList);
			return blocks;
		}

		private void CheckReplicas(IDictionary<long, Replica> expectedReplicas, BlockListAsLongs
			 decodedBlocks)
		{
			NUnit.Framework.Assert.AreEqual(expectedReplicas.Count, decodedBlocks.GetNumberOfBlocks
				());
			IDictionary<long, Replica> reportReplicas = new Dictionary<long, Replica>(expectedReplicas
				);
			foreach (BlockListAsLongs.BlockReportReplica replica in decodedBlocks)
			{
				NUnit.Framework.Assert.IsNotNull(replica);
				Replica expected = Sharpen.Collections.Remove(reportReplicas, replica.GetBlockId(
					));
				NUnit.Framework.Assert.IsNotNull(expected);
				NUnit.Framework.Assert.AreEqual("wrong bytes", expected.GetNumBytes(), replica.GetNumBytes
					());
				NUnit.Framework.Assert.AreEqual("wrong genstamp", expected.GetGenerationStamp(), 
					replica.GetGenerationStamp());
				NUnit.Framework.Assert.AreEqual("wrong replica state", expected.GetState(), replica
					.GetState());
			}
			NUnit.Framework.Assert.IsTrue(reportReplicas.IsEmpty());
		}

		[NUnit.Framework.Test]
		public virtual void TestCapabilitiesInited()
		{
			NamespaceInfo nsInfo = new NamespaceInfo();
			NUnit.Framework.Assert.IsTrue(nsInfo.IsCapabilitySupported(NamespaceInfo.Capability
				.StorageBlockReportBuffers));
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDatanodeDetect()
		{
			AtomicReference<DatanodeProtocolProtos.BlockReportRequestProto> request = new AtomicReference
				<DatanodeProtocolProtos.BlockReportRequestProto>();
			// just capture the outgoing PB
			DatanodeProtocolPB mockProxy = Org.Mockito.Mockito.Mock<DatanodeProtocolPB>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_205(request)).When(mockProxy).BlockReport
				(Matchers.Any<RpcController>(), Matchers.Any<DatanodeProtocolProtos.BlockReportRequestProto
				>());
			DatanodeProtocolClientSideTranslatorPB nn = new DatanodeProtocolClientSideTranslatorPB
				(mockProxy);
			DatanodeRegistration reg = DFSTestUtil.GetLocalDatanodeRegistration();
			NamespaceInfo nsInfo = new NamespaceInfo(1, "cluster", "bp", 1);
			reg.SetNamespaceInfo(nsInfo);
			Replica r = new FinalizedReplica(new Block(1, 2, 3), null, null);
			BlockListAsLongs bbl = BlockListAsLongs.Encode(Sharpen.Collections.Singleton(r));
			DatanodeStorage storage = new DatanodeStorage("s1");
			StorageBlockReport[] sbr = new StorageBlockReport[] { new StorageBlockReport(storage
				, bbl) };
			// check DN sends new-style BR
			request.Set(null);
			nsInfo.SetCapabilities(NamespaceInfo.Capability.StorageBlockReportBuffers.GetMask
				());
			nn.BlockReport(reg, "pool", sbr, new BlockReportContext(1, 0, Runtime.NanoTime())
				);
			DatanodeProtocolProtos.BlockReportRequestProto proto = request.Get();
			NUnit.Framework.Assert.IsNotNull(proto);
			NUnit.Framework.Assert.IsTrue(proto.GetReports(0).GetBlocksList().IsEmpty());
			NUnit.Framework.Assert.IsFalse(proto.GetReports(0).GetBlocksBuffersList().IsEmpty
				());
			// back up to prior version and check DN sends old-style BR
			request.Set(null);
			nsInfo.SetCapabilities(NamespaceInfo.Capability.Unknown.GetMask());
			nn.BlockReport(reg, "pool", sbr, new BlockReportContext(1, 0, Runtime.NanoTime())
				);
			proto = request.Get();
			NUnit.Framework.Assert.IsNotNull(proto);
			NUnit.Framework.Assert.IsFalse(proto.GetReports(0).GetBlocksList().IsEmpty());
			NUnit.Framework.Assert.IsTrue(proto.GetReports(0).GetBlocksBuffersList().IsEmpty(
				));
		}

		private sealed class _Answer_205 : Answer<DatanodeProtocolProtos.BlockReportResponseProto
			>
		{
			public _Answer_205(AtomicReference<DatanodeProtocolProtos.BlockReportRequestProto
				> request)
			{
				this.request = request;
			}

			public DatanodeProtocolProtos.BlockReportResponseProto Answer(InvocationOnMock invocation
				)
			{
				object[] args = invocation.GetArguments();
				request.Set((DatanodeProtocolProtos.BlockReportRequestProto)args[1]);
				return ((DatanodeProtocolProtos.BlockReportResponseProto)DatanodeProtocolProtos.BlockReportResponseProto
					.NewBuilder().Build());
			}

			private readonly AtomicReference<DatanodeProtocolProtos.BlockReportRequestProto> 
				request;
		}
	}
}
