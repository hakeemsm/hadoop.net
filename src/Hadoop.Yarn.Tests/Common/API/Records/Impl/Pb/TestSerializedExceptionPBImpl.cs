using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class TestSerializedExceptionPBImpl
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSerializedException()
		{
			SerializedExceptionPBImpl orig = new SerializedExceptionPBImpl();
			orig.Init(new Exception("test exception"));
			YarnProtos.SerializedExceptionProto proto = orig.GetProto();
			SerializedExceptionPBImpl deser = new SerializedExceptionPBImpl(proto);
			NUnit.Framework.Assert.AreEqual(orig, deser);
			NUnit.Framework.Assert.AreEqual(orig.GetMessage(), deser.GetMessage());
			NUnit.Framework.Assert.AreEqual(orig.GetRemoteTrace(), deser.GetRemoteTrace());
			NUnit.Framework.Assert.AreEqual(orig.GetCause(), deser.GetCause());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeserialize()
		{
			Exception ex = new Exception("test exception");
			SerializedExceptionPBImpl pb = new SerializedExceptionPBImpl();
			try
			{
				pb.DeSerialize();
				NUnit.Framework.Assert.Fail("deSerialze should throw YarnRuntimeException");
			}
			catch (YarnRuntimeException e)
			{
				NUnit.Framework.Assert.AreEqual(typeof(TypeLoadException), e.InnerException.GetType
					());
			}
			pb.Init(ex);
			NUnit.Framework.Assert.AreEqual(ex.ToString(), pb.DeSerialize().ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBeforeInit()
		{
			YarnProtos.SerializedExceptionProto defaultProto = ((YarnProtos.SerializedExceptionProto
				)YarnProtos.SerializedExceptionProto.NewBuilder().Build());
			SerializedExceptionPBImpl pb1 = new SerializedExceptionPBImpl();
			NUnit.Framework.Assert.IsNull(pb1.GetCause());
			SerializedExceptionPBImpl pb2 = new SerializedExceptionPBImpl();
			NUnit.Framework.Assert.AreEqual(defaultProto, pb2.GetProto());
			SerializedExceptionPBImpl pb3 = new SerializedExceptionPBImpl();
			NUnit.Framework.Assert.AreEqual(defaultProto.GetTrace(), pb3.GetRemoteTrace());
		}
	}
}
