using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class TestYSCRecordFactory
	{
		[NUnit.Framework.Test]
		public virtual void TestPbRecordFactory()
		{
			RecordFactory pbRecordFactory = RecordFactoryPBImpl.Get();
			try
			{
				NodeHeartbeatRequest request = pbRecordFactory.NewRecordInstance<NodeHeartbeatRequest
					>();
				NUnit.Framework.Assert.AreEqual(typeof(NodeHeartbeatRequestPBImpl), request.GetType
					());
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to crete record");
			}
		}
	}
}
