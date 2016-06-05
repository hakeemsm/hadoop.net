using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public class TestRegisterNodeManagerResponse
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRoundTrip()
		{
			RegisterNodeManagerResponse resp = recordFactory.NewRecordInstance<RegisterNodeManagerResponse
				>();
			byte[] b = new byte[] { 0, 1, 2, 3, 4, 5 };
			MasterKey containerTokenMK = recordFactory.NewRecordInstance<MasterKey>();
			containerTokenMK.SetKeyId(54321);
			containerTokenMK.SetBytes(ByteBuffer.Wrap(b));
			resp.SetContainerTokenMasterKey(containerTokenMK);
			MasterKey nmTokenMK = recordFactory.NewRecordInstance<MasterKey>();
			nmTokenMK.SetKeyId(12345);
			nmTokenMK.SetBytes(ByteBuffer.Wrap(b));
			resp.SetNMTokenMasterKey(nmTokenMK);
			resp.SetNodeAction(NodeAction.Normal);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, resp.GetNodeAction());
			// Verifying containerTokenMasterKey
			NUnit.Framework.Assert.IsNotNull(resp.GetContainerTokenMasterKey());
			NUnit.Framework.Assert.AreEqual(54321, resp.GetContainerTokenMasterKey().GetKeyId
				());
			Assert.AssertArrayEquals(b, ((byte[])resp.GetContainerTokenMasterKey().GetBytes()
				.Array()));
			RegisterNodeManagerResponse respCopy = SerDe(resp);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, respCopy.GetNodeAction());
			NUnit.Framework.Assert.IsNotNull(respCopy.GetContainerTokenMasterKey());
			NUnit.Framework.Assert.AreEqual(54321, respCopy.GetContainerTokenMasterKey().GetKeyId
				());
			Assert.AssertArrayEquals(b, ((byte[])respCopy.GetContainerTokenMasterKey().GetBytes
				().Array()));
			// Verifying nmTokenMasterKey
			NUnit.Framework.Assert.IsNotNull(resp.GetNMTokenMasterKey());
			NUnit.Framework.Assert.AreEqual(12345, resp.GetNMTokenMasterKey().GetKeyId());
			Assert.AssertArrayEquals(b, ((byte[])resp.GetNMTokenMasterKey().GetBytes().Array(
				)));
			respCopy = SerDe(resp);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, respCopy.GetNodeAction());
			NUnit.Framework.Assert.IsNotNull(respCopy.GetNMTokenMasterKey());
			NUnit.Framework.Assert.AreEqual(12345, respCopy.GetNMTokenMasterKey().GetKeyId());
			Assert.AssertArrayEquals(b, ((byte[])respCopy.GetNMTokenMasterKey().GetBytes().Array
				()));
		}

		/// <exception cref="System.Exception"/>
		public static RegisterNodeManagerResponse SerDe(RegisterNodeManagerResponse orig)
		{
			RegisterNodeManagerResponsePBImpl asPB = (RegisterNodeManagerResponsePBImpl)orig;
			YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto proto = asPB.GetProto
				();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			proto.WriteTo(@out);
			ByteArrayInputStream @in = new ByteArrayInputStream(@out.ToByteArray());
			YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto.Builder cp = YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto
				.NewBuilder();
			cp.MergeFrom(@in);
			return new RegisterNodeManagerResponsePBImpl(((YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto
				)cp.Build()));
		}
	}
}
